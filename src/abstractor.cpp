// The project aims to communicate with a child process through pipes.
// stdin, stdout and stderr of the child process are binded with three pipes
// and parent process sends inputs and takes outputs through corresponding pipes.
//
// @author: Mehmet Akın Elden

#include <iostream>
#include <fstream>
#include <string>
#include <pthread.h>
#include <vector>
#include <queue>
#include <shared_mutex>

using namespace std;

template <typename T>
struct safe_data
{
    mutable shared_mutex mutex_;
    T data;
};

struct input_parameters
{
    int nThread, nAbstract, nResult;
    safe_data<vector<string>> query;
    safe_data<queue<string>> abstracts;
};

struct thread_parameters
{
    char name;
    int nResult;
    string outputFile;
    safe_data<vector<string>> query;
    safe_data<queue<string>> abstracts;
};

struct result
{
    string file;
    double score;
    string summary;
};

vector<result>* process_abstracts(thread_parameters* params);
input_parameters get_input_params(string& inputFile);
pthread_t* create_start_thread(char name, string& outputFile, input_parameters inputParams);
bool compare_write_results(vector<result> results, string& outputFile);

int main(int argc, char *argv[])
{
    if (argc < 3)
	{
		printf("input and output file name arguments are required.");
		return 1;
	}

    // the path of the input file
    string inputFile = string(argv[1]);
    // the path of the output file
    string outputFile = string(argv[2]);

    auto params = get_input_params(inputFile);

    vector<pthread_t*> threads;
    threads.reserve(params.nThread);
    char letter = 'A';
    for (int i=0; i<params.nThread; i++)
    {
        pthread_t* pThread = create_start_thread(letter+i, outputFile, params);
        if (!pThread)
        {
            printf("Couldn't create thread %s", letter+i);
            exit(1);
        }
        else
            threads.push_back(pThread);
    }

    vector<result> results;
    results.reserve(params.nThread * params.nResult);

    for (auto pThread : threads)
    {
        vector<result>* res;
        pthread_join(*pThread, (void**)&res);
        for (auto r : *res)
            results.push_back(r);
    }

    bool status = compare_write_results(results, outputFile);
    if (status)
        return 0;
    else
        return 1;
}

pthread_t* create_start_thread(char name, string& outputFile, input_parameters inputParams)
{
    pthread_t* thread = new pthread_t();
    thread_parameters* tParams = new thread_parameters{ name, inputParams.nResult, outputFile, inputParams.query, inputParams.abstracts};
    int status = pthread_create(thread, nullptr, (void* (*)(void*))process_abstracts, (void*)tParams);
    if (status != 0)
        return nullptr;
    return thread;
}