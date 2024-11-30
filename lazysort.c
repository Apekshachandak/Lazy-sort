#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include<limits.h>
#include<time.h>
#define MAX_FILES 10010
#define MAX_NAME_LENGTH 9
#define FILES_PER_THREAD 20 // Fixed number of files per thread
#define COUNT_SORT_THRESHOLD 42
#define NAME_LENGTH 8
int sort_by_id;
typedef struct {
    char name[MAX_NAME_LENGTH];
    int id;
    char timestamp[20];
     unsigned long long int name_hash;
     unsigned long long int timestamp_value;   // ISO 8601 format
} File;

typedef struct {
    File *files;
    int start;
    int end;
    int max_id;
    unsigned long long int min_hash; // Minimum hash value
    unsigned long long int max_hash; 
    File *sorted_output;
    int sort_by_id;
} ThreadData;

unsigned long long int hash_name(const char *name) {
    unsigned long long int hash = 0;
    for (int i = 0; i < NAME_LENGTH && name[i] != '\0'; i++) {
        hash = hash * 26 + (unsigned long long int)name[i];
    }
    return hash;
}
// Function to parse timestamp to time_t

void *count_sortfortime(void *arg) {
    ThreadData *data = (ThreadData *)arg;
    int n = data->end - data->start;
    
    // Calculate range based on min and max timestamps
    unsigned long long int timestamp_range = data->max_hash - data->min_hash + 1;

    // Adjust the count array size based on the timestamp range
    unsigned long long int *count = (unsigned long long int *)calloc(timestamp_range, sizeof(unsigned long long int));
    if (!count) {
        printf("Memory allocation failed for count array\n");
        exit(0);
    }

    // Count occurrences of each timestamp in the segment
    for (int i = data->start; i < data->end; i++) {
        unsigned long long int timestamp_val = data->files[i].timestamp_value;
        count[timestamp_val - data->min_hash]++;
    }

    // Transform count array to prefix sums
    for (unsigned long long int i = 1; i < timestamp_range; i++) {
        count[i] += count[i - 1];
    }

    // Build the sorted output for this segment
    for (int i = n - 1; i >= 0; i--) {
        int idx = data->start + i;
        unsigned long long int timestamp_val = data->files[idx].timestamp_value;
        data->sorted_output[--count[timestamp_val - data->min_hash]] = data->files[idx];
    }

    free(count);
    return NULL;
}
void merge_sorted_sectionsfortime(File *output, File *parts[], int sizes[], int num_parts) {
    int *indices = (int *)calloc(num_parts, sizeof(int));
    int total_size = 0;

    for (int i = 0 ; i < num_parts; i++) {
        total_size += sizes[i];
    }

    for (int i = 0; i < total_size; i++) {
        int min_index = -1;
        for (int j = 0; j < num_parts; j++) {
            if (indices[j] < sizes[j]) {
                unsigned long long int left_time = parts[j][indices[j]].timestamp_value;
                unsigned long long int min_time = (min_index == -1) ? left_time : parts[min_index][indices[min_index]].timestamp_value;

                if (min_index == -1 || left_time < min_time) {
                    min_index = j;
                }
            }
        }
        output[i] = parts[min_index][indices[min_index]];
        indices[min_index]++;
    }

    free(indices);
}
// time_t parse_timestamp(const char *timestamp) {
//     struct tm tm;
//     memset(&tm, 0, sizeof(struct tm));
//     sscanf(timestamp, "%4d-%2d-%2dT%2d:%2d:%2d",
//            &tm.tm_year, &tm.tm_mon, &tm.tm_mday,
//            &tm.tm_hour, &tm.tm_min, &tm.tm_sec);

//     tm.tm_year -= 1900; // Adjust years since 1900
//     tm.tm_mon -= 1;     // Adjust month range (0-11)

//     return mktime(&tm);
// }
unsigned long long int timestamp_to_integer(const char *timestamp) {
    char cleaned_timestamp[20];
    int j = 0;

    // Remove non-numeric characters
    for (int i = 0; timestamp[i] != '\0'; i++) {
        if (timestamp[i] >= '0' && timestamp[i] <= '9') {
            cleaned_timestamp[j++] = timestamp[i];
        }
    }
    cleaned_timestamp[j] = '\0'; // Null terminate the cleaned string

    // Convert the cleaned timestamp to an unsigned long long integer
    return atoll(cleaned_timestamp);
}
void *count_sort1(void *arg);
void merge_sort(File files[], int start, int end);
void merge(File files[], int start, int mid, int end);
void merge_sorted_sectionsforcount(File *output, File *parts[], int sizes[], int num_parts,int sort_choice);
void merge_sorted_sectionsformerge(File *output, File *parts[], int sizes[], int num_parts,int sort_choice);

void print_files(File files[], int n);
void *merge_sort_thread(void *arg);
int main() {
    int num_files;
    File files[MAX_FILES];
// return 0;
    // Read number of files
    scanf("%d", &num_files);
    getchar(); // Consume newline character

    // Read file data
    for (int i = 0; i < num_files; i++) {
        scanf("%s %d %s", files[i].name, &files[i].id, files[i].timestamp);
    }
  char sort_choice[10];
   
    scanf("%s", sort_choice);
    printf("%s\n",sort_choice);
    if(strcmp(sort_choice, "ID") == 0)
    {
        sort_by_id =2;
    }
    else if(strcmp(sort_choice, "TIMESTAMP") == 0)
    {
        sort_by_id=3;
    }
     else if(strcmp(sort_choice, "NAME") == 0)
    {
        sort_by_id=1;
    }
    else{
        printf("wrong column chosen");
        exit(0);
    }
      int num_threads = (num_files + FILES_PER_THREAD - 1) / FILES_PER_THREAD; // Ceiling division

    // Prepare thread data
    pthread_t threads[num_threads];
    ThreadData thread_data[num_threads];
    File *sorted_parts[num_threads];
    int sizes[num_threads];
    if (num_files < COUNT_SORT_THRESHOLD) { 
        int max_id = 0;
          unsigned long long int max_hash = 0;
    unsigned long long int min_hash = ULLONG_MAX;
        if(sort_by_id==2)
        {
           
    for (int i = 0; i < num_files; i++) {
        if (files[i].id > max_id) {
            max_id = files[i].id;
        }
    }
        }
        else if(sort_by_id==1)
        {
        // Initialize to maximum possible value
    for (int i = 0; i < num_files; i++) {
        // scanf("%s %d %s", files[i].name, &files[i].id, files[i].timestamp);
        files[i].name_hash = hash_name(files[i].name);
        // printf("%lld\n", files[i].name_hash); // Hash file name
        if (files[i].name_hash > max_hash) {
            max_hash = files[i].name_hash;
        }
        if (files[i].name_hash < min_hash) {
            min_hash = files[i].name_hash; // Update min_hash
        }
    }  
        }
        else if (sort_by_id==3)
        {
             for (int i = 0; i < num_files; i++) {
        // scanf("%s %d %s", files[i].name, &files[i].id, files[i].timestamp);
        files[i].timestamp_value = timestamp_to_integer(files[i].timestamp);
        if (files[i].timestamp_value > max_hash) {
            max_hash = files[i].timestamp_value;
        }
        if (files[i].timestamp_value < min_hash) {
            min_hash= files[i].timestamp_value;
        }
    }
        }
    // Divide work among threads
    // printf("is it coming here ");
    for (int i = 0; i < num_threads; i++) {
        int start = i * FILES_PER_THREAD;
        int end = (start + FILES_PER_THREAD < num_files) ? start + FILES_PER_THREAD : num_files;

        sorted_parts[i] = (File *)malloc((end - start) * sizeof(File));
        sizes[i] = end - start;
        thread_data[i].files = files;
        thread_data[i].start = start;
        thread_data[i].end = end;
        thread_data[i].max_id = max_id;
        thread_data[i].min_hash = min_hash; // Pass min_hash to thread
        thread_data[i].max_hash = max_hash;
        thread_data[i].sorted_output = sorted_parts[i];
        thread_data[i].sort_by_id = sort_by_id; // Set sorting preference
        if(sort_by_id==3)
        {   if(max_hash-min_hash>1000000000)
        {
            pthread_create(&threads[i], NULL, merge_sort_thread, (void *)&thread_data[i]);
        }
        else{
            pthread_create(&threads[i], NULL, count_sortfortime, (void *)&thread_data[i]);}}
else{  
      pthread_create(&threads[i], NULL, count_sort1, (void *)&thread_data[i]);

    }
    }
    
    // Wait for all threads to finish
    for (int i = 0; i < num_threads; i++) {
        pthread_join(threads[i], NULL);
    }
    
// return(0);
    // Merge the sorted parts
    File *final_output = (File *)malloc(num_files * sizeof(File));
    if(sort_by_id==3)
    {
    merge_sorted_sectionsfortime(final_output, sorted_parts, sizes, num_threads);
    }
    else{
        merge_sorted_sectionsforcount(final_output, sorted_parts, sizes, num_threads,sort_by_id);}
    //  return 0;
    memcpy(files, final_output, num_files * sizeof(File));
    free(final_output);
    // Free allocated memory
    for (int i = 0; i < num_threads; i++) {
        free(sorted_parts[i]);
    }
    } else {
  
    // Divide work among threads
    for (int i = 0; i < num_threads; i++) {
        int start = i * FILES_PER_THREAD;
        int end = (start + FILES_PER_THREAD < num_files) ? start + FILES_PER_THREAD : num_files;

        sorted_parts[i] = (File *)malloc((end - start) * sizeof(File));
        sizes[i] = end - start;
        thread_data[i].files = files;
        thread_data[i].start = start;
        thread_data[i].end = end;
        thread_data[i].sorted_output = sorted_parts[i];
        // printf("is it coming here");
        pthread_create(&threads[i], NULL, merge_sort_thread, (void *)&thread_data[i]);
    }

    // Wait for all threads to finish
    for (int i = 0; i < num_threads; i++) {
        pthread_join(threads[i], NULL);
    }

    // Merge the sorted parts
    File *final_output = (File *)malloc(num_files * sizeof(File));
    merge_sorted_sectionsformerge(final_output, sorted_parts, sizes, num_threads,sort_by_id);
        // Copy final output back to files array
        // return 0;
        memcpy(files, final_output, num_files * sizeof(File));
        free(final_output);

        // Free allocated memory
        for (int i = 0; i < num_threads; i++) {
            free(sorted_parts[i]);
        }
    }

    // Print the sorted files
    print_files(files, num_files);
    return 0;
}

void *count_sort1(void *arg) {
    ThreadData *data = (ThreadData *)arg;
    int n = data->end - data->start;
    unsigned long long int *count;
    unsigned long long int range;
    if(data->sort_by_id==2 )
    {
    count = (unsigned long long int *)calloc(data->max_id + 1, sizeof(unsigned long long int ));}
    else if ( sort_by_id == 1|| sort_by_id == 3){
    range = data->max_hash - data->min_hash + 1;
    count = (unsigned long long int *)calloc(range, sizeof(unsigned long long int));
    if (!count) {
        perror("Failed to allocate memory for count array");
        pthread_exit(NULL);
    }
    }
   
    // Count occurrences of each ID in the segment
    for (int i = data->start; i < data->end; i++) {
        if(data->sort_by_id==2)
        count[data->files[i].id]++;
        else if(data->sort_by_id==1)
         count[data->files[i].name_hash - data->min_hash]++;
         else if (data->sort_by_id == 3)
         {
             unsigned long long int timestamp_val = data->files[i].timestamp_value;
        count[timestamp_val - data->min_hash]++;
         }
    }

    // Transform count array to prefix sums
    if(data->sort_by_id==2)
   { for (int i = 1; i <= data->max_id; i++) {
        count[i] += count[i - 1];
    }}
    else if(data->sort_by_id==1 || data ->sort_by_id == 3)
    {
          for (unsigned long long int i = 1; i < range; i++) {
        count[i] += count[i - 1];
    }
    }

    // Build the sorted output for this segment
    for (int i = n - 1; i >= 0; i--) {
        if(data->sort_by_id==2)
        {
        int idx = data->start + i;
        data->sorted_output[--count[data->files[idx].id]] = data->files[idx];}
        else if(data->sort_by_id==1){
             unsigned long long int idx = data->start + i;
        data->sorted_output[--count[data->files[idx].name_hash - data->min_hash]] = data->files[idx];
        }
        else if (data ->sort_by_id == 3)
        {
             int idx = data->start + i;
        unsigned long long int timestamp_val = data->files[idx].timestamp_value;
        data->sorted_output[--count[timestamp_val - data->min_hash]] = data->files[idx];
        }
    }

    free(count);
    return NULL;
}


// Function for sorting using merge sort within each thread
void *merge_sort_thread(void *arg) {
    ThreadData *data = (ThreadData *)arg;
    int n = data->end - data->start;

    // Copy the segment to be sorted by this thread
    for (int i = 0; i < n; i++) {
        data->sorted_output[i] = data->files[data->start + i];
    }

    // Perform merge sort on this segment
    merge_sort(data->sorted_output, 0, n - 1);

    return NULL;
}

// Recursive merge sort function
void merge_sort(File files[], int start, int end) {
    if (start < end) {
        int mid = start + (end - start) / 2;
        merge_sort(files, start, mid);
        merge_sort(files, mid + 1, end);
        merge(files, start, mid, end);
    }
}

// Function to merge two sorted halves
void merge(File files[], int start, int mid, int end) {
    int n1 = mid - start + 1;
    int n2 = end - mid;

    File *left = (File *)malloc(n1 * sizeof(File));
    File *right = (File *)malloc(n2 * sizeof(File));

    for (int i = 0; i < n1; i++) {
        left[i] = files[start + i];
    }
    for (int j = 0; j < n2; j++) {
        right[j] = files[mid + 1 + j];
    }

    int i = 0, j = 0, k = start;
    while (i < n1 && j < n2) { 
        if(sort_by_id==2)
        {
        if (left[i].id <= right[j].id) {
            files[k++] = left[i++];
        } else {
            files[k++] = right[j++];
        }
        }
        else if(sort_by_id==1)
        {
             if (strcmp(left[i].name, right[j].name) <= 0) {
            files[k++] = left[i++];
        } else {
            files[k++] = right[j++];
        }
        }
        if(sort_by_id==3)
        {
             if (strcmp(left[i].timestamp, right[j].timestamp) <= 0) {
            files[k++] = left[i++];
        } else {
            files[k++] = right[j++];
        }
        }
    }

    while (i < n1) {
        files[k++] = left[i++];
    }
    while (j < n2) {
        files[k++] = right[j++];
    }

    free(left);
    free(right);
}

// Function to merge sorted sections from different threads
void merge_sorted_sectionsforcount(File *output, File *parts[], int sizes[], int num_parts,int sort_choice) {
    int *indices = (int *)calloc(num_parts, sizeof(int));
    int total_size = 0;

    for (int i = 0; i < num_parts; i++) {
        total_size += sizes[i];
    }

    for (int i = 0; i < total_size; i++) {
        int min_index = -1;
        for (int j = 0; j < num_parts; j++) {
            if (indices[j] < sizes[j]) {
                if(sort_choice==2)
                {
                if (min_index == -1 || parts[j][indices[j]].id < parts[min_index][indices[min_index]].id) {
                    min_index = j;
                }
                }
                else if (sort_choice==1){ 
                    if (min_index == -1 || parts[j][indices[j]].name_hash < parts[min_index][indices[min_index]].name_hash) {
                    min_index = j;
                }
                else if (sort_choice==3)
                {
                   if (min_index == -1 || parts[j][indices[j]].timestamp_value < parts[min_index][indices[min_index]].timestamp_value) {
                    min_index = j;
                   }
                }
                }
            }
        }
        output[i] = parts[min_index][indices[min_index]];
        indices[min_index]++;
    }
   
    free(indices);
}
void merge_sorted_sectionsformerge(File *output, File *parts[], int sizes[], int num_parts,int sort_choice) {
    int *indices = (int *)calloc(num_parts, sizeof(int));
    int total_size = 0;

    for (int i = 0; i < num_parts; i++) {
        total_size += sizes[i];
    }

    for (int i = 0; i < total_size; i++) {
        int min_index = -1;
        for (int j = 0; j < num_parts; j++) {
            if (indices[j] < sizes[j]) {
                if(sort_choice ==2 )
                {
                if (min_index == -1 || parts[j][indices[j]].id < parts[min_index][indices[min_index]].id) {
                    min_index = j;
                }
                }
                else if (sort_choice==1){ 
                    if (min_index == -1 || strcmp(parts[j][indices[j]].name, parts[min_index][indices[min_index]].name) < 0) {
                    min_index = j;
                }
                }
                 else if (sort_choice==3)
                {
                   if (min_index == -1 || strcmp(parts[j][indices[j]].timestamp, parts[min_index][indices[min_index]].timestamp) < 0) {
                    min_index = j;
                }
                }
            }
        }
        output[i] = parts[min_index][indices[min_index]];
        indices[min_index]++;
    }

    free(indices);
}
// Function to print files in the expected format
void print_files(File files[], int n) {
    for (int i = 0; i < n; i++) {
        printf("%s %d %s\n", files[i].name, files[i].id, files[i].timestamp);
    }
}
