typedef struct data {
    int length;
    void *data;
} db_data;

typedef struct indices {
    int num_indices;
    int *indices;
} indices;

void *create_db();
void free_db(void *db);

// Returns the index of the data in the database.
int append_object_sync(void *db, db_data object);

typedef void (*write_cb)(void *, int);

void append_object_async(void *db, db_data object, write_cb callback, void *cb_arg);
struct read_response {
    int err;
    db_data data;
};

typedef void (*read_cb)(void *, struct read_response);

// Read object at given index
struct read_response read_object_sync(void *db, int index);

//
void read_object_async(void *db, int index, read_cb callback, void *cb_arg);

enum write_err {
    WRITE_SUCCESSFUL,
    KEY_TOO_LONG,
    WRITE_IO_ERROR,
    GENERIC_WRITE_ERROR,
}

typedef void (*key_write_cb)(void *, enum write_err); // cb_arg and

void write_value_async(void *db, db_data key, db_data value, key_write_cb callback, void *cb_arg);

enum read_err {
    READ_SUCCESSFUL,
    KEY_NOT_FOUND,
    READ_IO_ERROR,
    GENERIC_READ_ERROR,
}

typedef void (*key_read_cb)(void *, enum read_err, db_data);
// cb_arg, err, value

void read_value_async(void *db, db_data key, read_cb callback, void *cb_arg);

void poll(void *db);
