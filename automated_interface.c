#include "db_interface.h"

#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/types.h>

#include <stdatomic.h>
#include <stdbool.h>

struct read_cb_data {
    db_data key;
    db_data expected_value;
};

_Atomic int errors = 0;

void write_callback(void *cb_arg, enum write_err error) {
    struct read_cb_data *data = cb_arg;

    if (error != WRITE_SUCCESSFUL) {
        printf("\n\nGOT ERROR ON WRITE: %d\n\n", error);
        errors++;
    }
    
    free(data -> key.data);
    free(data -> expected_value.data);
    free(data);
}

void read_cb(void *cb_arg, enum read_err error, db_data value) {
    struct read_cb_data *data = cb_arg;
    if (error != READ_SUCCESSFUL) {
        printf("GOT ERROR READING: %d for key %.16s\n", error, data -> key.data);
        errors++;
        goto exit;
    }

    if (data -> expected_value.length != value.length) {
        printf("Expected length would be %d but was %d for key %.16s\n", data -> expected_value.length, value.length, data -> key.data);
        errors++;
        goto exit;
    }

    if (memcmp(value.data, data -> expected_value.data, value.length) != 0) {
        printf("Expected data not what was received for key %.16s\n", data -> key.data);
        printf("got:      %.64s (%d)\n", value.data, value.length);
        printf("expected: %.64s (%d)\n", data -> expected_value.data, data -> expected_value.length);
        errors++;
        goto exit;
    }

exit:
#ifdef DEBUG
    printf("Read completed with err %d!\n", error);
#endif
    free(data -> key.data);
    free(data -> expected_value.data);
    free(data);
}

static short halfbyte(char halfbyte) {
    if (halfbyte < 10) {
        return 48 + halfbyte;
    }
    return 97 + halfbyte-10;
}

static short byte_to_hex(unsigned char byte) {
    short firstletter = halfbyte(byte & 15);
    byte >>=4;
    short secondletter = halfbyte(byte);
    return (secondletter<<8) + (firstletter);
}

struct data_generator {
    _Atomic void *data; // Each datas() has 64kb of random numbers
    _Atomic int datas_valid;

    _Atomic bool reset;
};

static char *random_bytes(struct data_generator *gen, int num_bytes) {
    unsigned int num_ints = (num_bytes>>2) + ((num_bytes&3) ? 1 : 0);
    void *buf = malloc(num_ints*sizeof(int));
    int buf_location = 0;
    while (num_bytes) {
        while (!gen -> data) {
            usleep(1000);
        }
        int gen_bytes = (64*1024) - gen -> datas_valid;
        int min_bytes = gen_bytes < num_bytes ? gen_bytes : num_bytes;
        memcpy(buf + buf_location, gen -> data + gen -> datas_valid, min_bytes);
        buf_location += min_bytes;
        num_bytes -= min_bytes;
        gen -> datas_valid += min_bytes;
        if (gen -> datas_valid == (64*1024)) {
            free(gen -> data);
            gen -> data = NULL;
        }
    }
    return buf;
}

db_data generate_key(struct data_generator *gen) {
    unsigned int key_exp = (random()) % 7 + 4; // e.g. 5, so key is 2<<5 bytes = 32.
    unsigned int key_len = (2<<key_exp) + (16-(random()%32));
    // key_len = 16;
    db_data key = {.length=key_len, .data=random_bytes(gen, key_len)};
    return key;
}

db_data generate_data(struct data_generator *gen) {
    unsigned int data_exp = (random() % 9) + 6;
    unsigned int data_len = (2<<data_exp) + ((1<<(data_exp-2))-(random()%(1<<(data_exp-1))));
    // data_len = 64;
    db_data value = {.length=data_len, .data=random_bytes(gen, data_len)};
    return value;
}

unsigned long long GetTimeStamp() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec*(unsigned long long)1000000+tv.tv_usec;
}

int data_thread(struct data_generator *generator) {
    printf("data thread started!\n");
    unsigned int data_seed = 8901;
    struct random_data buffer;
    char   random_state[128];
    memset(&buffer, 0, sizeof(struct random_data));
    memset(random_state, 0, sizeof(random_state));
    initstate_r(data_seed,random_state,sizeof(random_state),&buffer);
    srandom_r(data_seed, &buffer);

    while (!generator -> reset) {
        _Atomic void *data = malloc(64*1024);
        for (int i = 0; i < (64*1024); i+=4) {
            random_r(random_state, data + i);
        }

        if (generator -> data == 0) {
            generator -> data = data;
        }
    }

    srandom_r(data_seed, &buffer);
    while (1) {
        int *data = malloc(64*1024);
        for (int i = 0; i < (16*1024); i++) {
            random_r(random_state, &data[i]);
        }

        if (generator -> data == 0) {
            generator -> datas_valid = 0;
            generator -> data = data;
        }
    }

    return 0;
}

int main(int argc, char **argv) {
    unsigned int seed = 5678;
    int num_keys = atoi(argv[1]);
    printf("%d keys. pid %d\n", num_keys, getpid());
    pthread_t thread_id = 0;
    struct data_generator *data_gen = calloc(sizeof(struct data_generator), 1);
    pthread_create(&thread_id, NULL, data_thread, data_gen);
    void *db = create_db();
    srandom(seed);
    int cpu_begin = clock();
    unsigned long long wall_begin = GetTimeStamp();
    unsigned long long bytes_written = 0;
    for (int i = 0; i < num_keys; i++) {
        db_data key = generate_key(data_gen);
        db_data value = generate_data(data_gen);
        bytes_written += key.length + value.length + 7;
        struct read_cb_data *data = calloc(sizeof(struct read_cb_data), 1);
        data -> key = key;
        data -> expected_value = value;
        write_value_async(db, key, value, write_callback, data);
        // flush_commands(db);
        poll_db(db);
        /*
        for (int i = 0; i < waits; i++) {
            poll_db(db);
            usleep(1000);
        }
        */
        // wait_for_no_writes(db);
    }
    double cpu_diff = clock() - cpu_begin;
    double wall_diff = GetTimeStamp() - wall_begin;
    printf("Took %2.3g seconds of cpu time and %2.3g seconds of wall time to write %d keys and %llu bytes\n", cpu_diff/1000000.0, wall_diff/1000000.0, num_keys, bytes_written);

    data_gen -> reset = 1;
    for (int i = 0; i < 1000; i++) {
        poll_db(db);
        usleep(1000);
    }

    srandom(seed);
    for (int i = 0; i < num_keys; i++) {
        db_data key = generate_key(data_gen);
        db_data value = generate_data(data_gen);
        struct read_cb_data *data = calloc(sizeof(struct read_cb_data), 1);
        data -> key = key;
        data -> expected_value = value;
        read_value_async(db, key, read_cb, data);
    }

    for (int i = 0; i < 1000; i++) {
        poll_db(db);
        usleep(1000);
    }

    dump_sectors_to_file(db, 0, 10);

    for (int i = 0; i < 1000; i++) {
        poll_db(db);
        usleep(1000);
    }

    printf("Exiting! In total %d errors.\n", errors);
    free_db(db);
    return errors;
}