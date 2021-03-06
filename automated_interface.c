#include "db_interface.h"

#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <stdatomic.h>
#include <stdbool.h>

// Automated testing for the sillydb. At the moment this is primarily just for testing correctness,
// not performance (though I do intend to make it better for testing the latter).

struct read_cb_data {
    db_data key;
    db_data expected_value;
    unsigned long long time_at_issue;
};

int errors = 0;
unsigned long long total_write_latency = 0;
unsigned long long total_read_latency = 0;

unsigned long long get_time_us() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec*(unsigned long long)1000000+tv.tv_usec;
}

void write_callback(void *cb_arg, enum write_err error) {
    struct read_cb_data *data = cb_arg;

    if (error != WRITE_SUCCESSFUL) {
        printf("\n\nGOT ERROR ON WRITE: %d\n\n", error);
        errors++;
    }

    total_write_latency += get_time_us() - data -> time_at_issue;

    // free(data -> key.data);
    // free(data -> expected_value.data);
    free(data);
}

void read_cb(void *cb_arg, enum read_err error, db_data value) {
    struct read_cb_data *data = cb_arg;
    total_read_latency += get_time_us() - data -> time_at_issue;

    if (error != READ_SUCCESSFUL) {
        printf("GOT ERROR READING: %d for key %.16s\n", error, data -> key.data);
        errors++;
        return;
    }

    if (data -> expected_value.length != value.length) {
        printf("Expected length would be %d but was %d for key %.16s\n", data -> expected_value.length, value.length, data -> key.data);
        errors++;
        return;
    }

    if (memcmp(value.data, data -> expected_value.data, value.length) != 0) {
        printf("Expected data not what was received for key %.16s\n", data -> key.data);
        printf("got:      %.64s (%d)\n", value.data, value.length);
        printf("expected: %.64s (%d)\n", data -> expected_value.data, data -> expected_value.length);
        errors++;
        return;
    }
#ifdef DEBUG
    printf("Read completed with err %d!\n", error);
#endif
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

unsigned int generate_key_len() {
    unsigned int key_exp = (random() % 7) + 4; // e.g. 5, so key is 2<<5 bytes = 32.
    return (2<<key_exp) + ((1<<(key_exp-2))-(random()%(1<<(key_exp-1))));
}

unsigned int generate_data_len() {
    unsigned int data_exp = (random() % 9) + 6;
    return (2<<data_exp) + ((1<<(data_exp-2))-(random()%(1<<(data_exp-1))));
}

void *generate_entropy(int data_seed, unsigned long long length) {
    int fd = open("/dev/urandom", O_RDONLY);
    int *data = malloc(length>>1); // TODO: mark this memory unpageable.
    srandom(data_seed);
    for (int i = 0; i < (length/8); i++) {
        data[i] = random();
    }
    /*
    unsigned long long total_read = 0;
    while (total_read != length) {
        // reading from urandom will provide only 32mb at a time, so account for that.
        total_read += read(fd, data+total_read, (length>>1)-total_read);
    }
    */
    close(fd);
    char *char_data = data;
    short *bata = malloc(length);
    for (int i = 0; i < (length>>1); i++) {
        bata[i] = byte_to_hex(char_data[i]);
    }
    free(data);
    return (void *)bata;
}

// Stolen from stackoverflow:
void shuffle(struct read_cb_data *array, int n) {
    for (int i = 0; i < n - 1; i++) {
        int j = i + random() / (RAND_MAX / (n - i) + 1);
        struct read_cb_data t = array[j];
        array[j] = array[i];
        array[i] = t;
    }
}

int main(int argc, char **argv) {
    // TODO: implement mixed r/w workload, or full r/full w workloads, for perf testing.
    unsigned int seed = 1001;
    int num_keys = atoi(argv[1]);
    printf("%d keys. pid %d\n", num_keys, getpid());
    unsigned long long num_bytes = num_keys * 60000;
    void *entropy = generate_entropy(5678, num_keys*60000); // approximate maximum entropy needed. for 100k keys this is 4gb
    printf("Generated entropy\n");
    void *db = create_db();
    srandom(seed);
    int cpu_begin = clock();
    unsigned long long wall_begin = get_time_us();
    unsigned long long bytes_written = 0;
    unsigned long long entropy_used = 0;
    for (int i = 0; i < num_keys; i++) {
        unsigned int key_len = generate_key_len();
        db_data key = {.length=key_len, .data=entropy+entropy_used};
        entropy_used += key_len;

        unsigned int value_len = generate_data_len();
        db_data value = {.length=value_len, .data=entropy+entropy_used};
        entropy_used += value_len;

        bytes_written += key_len + value_len + 7;
        struct read_cb_data *data = calloc(sizeof(struct read_cb_data), 1);
        data -> key = key;
        data -> expected_value = value;
        data -> time_at_issue = get_time_us();
        write_value_async(db, key, value, write_callback, data);
        poll_db(db);
    }
    double cpu_diff = clock() - cpu_begin;
    double wall_diff = get_time_us() - wall_begin;
    printf("Took %2.3g seconds of cpu time and %2.3g seconds of wall time to write %d keys and %llu bytes\n", cpu_diff/1000000.0, wall_diff/1000000.0, num_keys, bytes_written);

    wait_for_zero_writes(db);

    /*
    // Let everything settle out, purge all writes etc.
    for (int i = 0; i < 30000; i++) {
        poll_db(db);
        usleep(1000);
    }
    */

    srandom(seed); // We only use this for the key len, but it's still important. This means we 
    // can exactly replicate the previous set of keys+values and we can make sure the db stored them
    // correctly.
    entropy_used = 0;

    struct read_cb_data *cbs = calloc(sizeof(struct read_cb_data), num_keys);
    for (int i = 0; i < num_keys; i++) {
        unsigned int key_len = generate_key_len();
        db_data key = {.length=key_len, .data=entropy+entropy_used};
        entropy_used += key_len;

        unsigned int value_len = generate_data_len();
        db_data value = {.length=value_len, .data=entropy+entropy_used};
        entropy_used += value_len;

        cbs[i].key = key;
        cbs[i].expected_value = value;
    }

    shuffle(cbs, num_keys);

    for (int i = 0; i < num_keys; i++) {
        cbs[i].time_at_issue = get_time_us();
        read_value_async(db, cbs[i].key, read_cb, cbs+i);
        for (int i = 0; i < 1000; i++) {
            poll_db(db);
        }
    }

    for (int i = 0; i < 1000; i++) {
        poll_db(db);
        usleep(1000);
    }

    // dump_sectors_to_file(db, 0, 10);

    for (int i = 0; i < 3000; i++) {
        poll_db(db);
        usleep(1000);
    }

    double avg_write_latency = ((double) total_write_latency)/num_keys;
    double avg_read_latency = ((double) total_read_latency)/num_keys;
    avg_write_latency/=1000.0;
    avg_read_latency/=1000.0;


    printf("Exiting! In total %d errors. Avg write latency: %.03gms. Avg read latency: %.03gms.\n", errors, avg_write_latency, avg_read_latency);
    free_db(db);
    free(cbs);
    free(entropy);
    return errors;
}