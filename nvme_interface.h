typedef struct data {
    int length;
    void *data;
} data;

typedef struct indices {
    int num_indices;
    int *indices;
} indices;

// Returns the index of the data in the database.
int append_object(data object);

// Read object at given index
data read_object(int index);

data *read_objects(indices indices);
