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
int append_object(void *db, db_data object);

struct read_response {
    int err;
    db_data data;
};

typedef void (*read_cb)(void *, void *, int);

// Read object at given index
struct read_response read_object_sync(void *db, int index);

//
struct read_response read_object_async(void *db, int index, read_cb callback, void *cb_arg);

// db_data *read_objects(void *db, indices indices);
