#include "wrapper.h"

void uzfs_seq_file_set_buffer(struct seq_file *sf, char *buf, int size)
{
    sf->buf = buf;
    sf->size = size;
}
