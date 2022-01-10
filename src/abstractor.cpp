// The project aims to communicate with a child process through pipes.
// stdin, stdout and stderr of the child process are binded with three pipes
// and parent process sends inputs and takes outputs through corresponding pipes.
//
// @author: Mehmet Akın Elden

#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <pthread.h>
#include <vector>
#include <queue>
#include <shared_mutex>

using namespace std;

template <typename T>
struct safe_data
{
    shared_mutex _mutex;
    T *data;
};

struct input_parameters
{
    int nThread, nAbstract, nResult;
    vector<string> *query;
    queue<string> *abstracts;
};

struct result
{
    string file;
    double score;
    string summary;
};

struct thread_parameters
{
    char name;
    int nResult;
    const string outputFile;
    safe_data<vector<string>> *query;
    safe_data<queue<string>> *abstracts;
    safe_data<vector<result>> *finalResults;
};

input_parameters get_input_params(string &inputFile);
pthread_t *create_start_thread(char name, const string &outputFile, int nResult, safe_data<vector<string>> *query, safe_data<queue<string>> *abstracts, safe_data<vector<result>> *finalResults);
void *process_abstracts(thread_parameters *params) { cout << "Thread " << params->name << endl; return nullptr; };
// bool compare_write_results(vector<result> &results, const string &outputFile);

int main(int argc, char *argv[])
{
    if (argc < 3)
    {
        cout << "input and output file name arguments are required." << endl;
        return 1;
    }

    // the path of the input file
    string inputFile = string(argv[1]);
    // the path of the output file
    string outputFile = string(argv[2]);

    auto params = get_input_params(inputFile);

    safe_data<vector<string>> query;
    {
        unique_lock lock(query._mutex);
        query.data = params.query;
    }

    safe_data<queue<string>> abstracts;
    {
        unique_lock lock(abstracts._mutex);
        abstracts.data = params.abstracts;
    }

    safe_data<vector<result>> finalResults;
    {
        unique_lock lock(finalResults._mutex);
        vector<result> *results = new vector<result>();
        results->reserve(params.nResult * 2);
        finalResults.data = results;
    }

    vector<pthread_t *> threads;
    threads.reserve(params.nThread);
    char letter = 'A';
    for (int i = 0; i < params.nThread; i++)
    {
        pthread_t *pThread = create_start_thread(letter + i, outputFile, params.nResult, &query, &abstracts, &finalResults);
        if (!pThread)
        {
            cout << "Couldn't create thread : " << letter + i << endl;
            exit(1);
        }
        else
            threads.push_back(pThread);
    }

    for (auto pThread : threads)
    {
        pthread_join(*pThread, NULL);
    }

    // bool status = compare_write_results(results, outputFile);
    // if (status)
    //     return 0;
    // else
    //     return 1;
    return 0;
}

vector<string> *tokenize_string(const string &line, const string &delim)
{
    string cp(line);
    auto tokens = new vector<string>();
    tokens->reserve(100);
    size_t index;
    do
    {
        index = cp.find(delim);
        // to prevent cases of double space etc.
        if (index > 0)
            tokens->push_back(cp.substr(0, index));
        cp.erase(0, index + delim.size());
    } while (index != string::npos);

    return tokens;
}

input_parameters get_input_params(string &inputFile)
{
    ifstream file(inputFile);
    if (!file)
    {
        cout << "Cannot open the file : " << inputFile << endl;
        exit(1);
    }

    // first line, integer parameters
    string line;
    getline(file, line);
    stringstream ss(line);
    int T, A, N;
    ss >> T >> A >> N;

    // second line, query
    getline(file, line);
    vector<string> *pQuery = tokenize_string(line, " ");

    queue<string> *pAbstracts = new queue<string>();
    // read following A number of lines
    int i = 0;
    while (getline(file, line) && i++ <= A)
    {
        pAbstracts->push(line);
    }

    input_parameters params = {T, A, N, pQuery, pAbstracts};
    return params;
}

pthread_t *create_start_thread(char name, const string &outputFile, int nResult, safe_data<vector<string>> *query, safe_data<queue<string>> *abstracts, safe_data<vector<result>> *finalResults)
{
    pthread_t *thread = new pthread_t();
    thread_parameters *tParams = new thread_parameters{name, nResult, outputFile, query, abstracts, finalResults};
    int status = pthread_create(thread, NULL, (void *(*)(void *))process_abstracts, (void *)tParams);
    if (status != 0)
        return nullptr;
    return thread;
}