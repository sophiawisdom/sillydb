typedef struct data {
    int length;
    void *data;
} db_data;

void *create_db(void);
void free_db(void *db);

enum write_err {
    WRITE_SUCCESSFUL,
    KEY_TOO_LONG,
    WRITE_IO_ERROR,
    NOT_ENOUGH_SPACE_ERROR,
    GENERIC_WRITE_ERROR,
};

typedef void (*key_write_cb)(void *, enum write_err); // cb_arg and

void write_value_async(void *db, db_data key, db_data value, key_write_cb callback, void *cb_arg);

enum read_err {
    READ_SUCCESSFUL,
    KEY_NOT_FOUND,
    READ_IO_ERROR,
    GENERIC_READ_ERROR,
};

typedef void (*key_read_cb)(void *, enum read_err, db_data);
// cb_arg, err, value

void read_value_async(void *db, db_data key, key_read_cb callback, void *cb_arg);

void poll_db(void *opaque);

void dump_sectors_to_file(void *opaque, int start_lba, int num_blocks);

void flush_commands(void *opaque);
