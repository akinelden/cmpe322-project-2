/** 
 * The project aims to process a list of files in a multi-threaded program.
 * The program reads the number of threads, abstract file list, query words 
 * and number of requested results parameters from the input file.
 * It puts the abstract file list into a FIFO queue, initializes a vector
 * for results and protects both of them with mutexes. Seperate threads
 * are created and these data structures are passed to them. Using read/write
 * mutex structures, each thread takes an abstract from the queue, process it
 * find the jaccard score, and return the result. Best results from each thread
 * is written to a global result vector when the thread finishes its job.
 * Finally when all threads are done, main thread writes the best results
 * to the output file.
 * 
 * @author: Mehmet Akın Elden
 */

#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <pthread.h>
#include <vector>
#include <queue>
#include <unordered_set>
#include <algorithm>
#include <shared_mutex>

using namespace std;

/**
 * This struct is used when a data is to be shared between 
 * threads. It contains a shared_mutex which provides
 * read/write lock mechanism and a generic data structure.
 * Before accessing the inner data element, one must lock
 * the _mutex with read or write lock.
 */
template <typename T>
struct shared_data
{
    shared_mutex _mutex;
    T *data;
};

/**
 * This struct is used to collect parameters inside 
 * input file. The data is not in shared_data struct since
 * input parameters are read in main thread.
 * 
 *  nThread : number of threads T
 *  nAbstract : number of abstracts A
 *  nResult : number of results N
 *  query : pointer to a vector containing query words
 *  abstracts : pointer to a FIFO queue containing list of abstract files
 */
struct input_parameters
{
    int nThread, nAbstract, nResult;
    vector<string> *query;
    queue<string> *abstracts;
};

/**
 * This struct is used to store result of a processed abstract.
 * 
 *  file : name of the abstract file
 *  score : jaccard score of the abstract file
 *  summary : sentences containing at least one word from query
 */
struct result
{
    string file;
    double score;
    string summary;
};

/**
 * This struct is used to store the parameters 
 * that are passed to the threads.
 * Abstracts queue and globalResults vector are stored in
 * shared_data struct since they are used and changed by thread
 * concurrently. However, since query vector is small (cost of 
 * cloning for each thread is small) and is not needed
 * to be changed, it is passed as a constant vector 
 * instead of shared_data.
 * 
 *  name : name of the thread like "A"
 *  nResult : number of results expected at the end of the program
 *  outputFile : the path of the output file
 *  query : the query vector
 *  abstracts : shared FIFO queue which contains abstracts list and protected with mutex
 *  globalResults : shared result vector which contains best results from threads and protected with mutex.
 *                  the logic behind using a globalResults structure is explained in the main.
 */
struct thread_parameters
{
    char name;
    int nResult;
    const string outputFile;
    const vector<string> query;
    shared_data<queue<string>> *abstracts;
    shared_data<vector<result>> *globalResults;
};

/**
 * Function used to get input parameters from the input file.
 * 
 * @param inputFile the path of the input file
 * @return the input parameters as a struct
 */
input_parameters get_input_params(const string &inputFile);

/**
 * Function used to create and start a thread. It packs the given parameters 
 * in a thread_parameters struct and passes it to newly created thread.
 * If thread is successfully created and started, returns the pointer
 * to the thread.
 * 
 * @param name name of the thread to be created
 * @param outputFile the path of the output file
 * @param nResult number of results expected from the program
 * @param query the query vector
 * @param abstracts queue for abstract list in a shared_data struct
 * @param globalResults vector for storing results in a shared_data struct
 * @return the pointer to the created thread
 */
pthread_t *create_start_thread(char name, const string &outputFile, int nResult, const vector<string> &query, shared_data<queue<string>> *abstracts, shared_data<vector<result>> *globalResults);

/**
 * Function executed inside each thread. Following actions are taken in this function:
 * 1. Thread local variables are initialized including the localResults vector
 *      which keeps the best N number of results of the thread.
 * 2. An abstract is popped from the queue (mutex required)
 * 3. The abstract is processed with process_abstract function
 * 4. The result is inserted to localResults vector if its score is high enough
 * 5. Go back to step 2 until the queue is emptied.
 * 6. Compare the local results with global results and insert the ones with higher score (mutex required)
 * 
 * @param params the thread parameters struct
 */
void *thread_process(thread_parameters *params);
result process_abstract(const char name, const string &outputFile, const string &abstract, const vector<string> &query, double minScore);
double jaccard_score(const string &abstractText, const vector<string> &query);
string get_summary(const string &abstractText, const vector<string> &query);
int insert_result(result res, vector<result> &results, int nResult, int startIndex = 0);

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

    {
        ofstream file(outputFile, ios::out);
        file << "";
        file.close();
    }

    auto params = get_input_params(inputFile);

    shared_data<queue<string>> abstracts;
    {
        unique_lock lock(abstracts._mutex);
        abstracts.data = params.abstracts;
    }

    shared_data<vector<result>> globalResults;
    {
        unique_lock lock(globalResults._mutex);
        vector<result> *results = new vector<result>();
        results->reserve(params.nResult * 2);
        globalResults.data = results;
    }

    vector<pthread_t *> threads;
    threads.reserve(params.nThread);
    char letter = 'A';
    for (int i = 0; i < params.nThread; i++)
    {
        pthread_t *pThread = create_start_thread(letter + i, outputFile, params.nResult, *params.query, &abstracts, &globalResults);
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

    {
        ofstream file(outputFile, ios::app);
        file.precision(4);
        file << "###" << endl;
        
        unique_lock lock(globalResults._mutex);
        int i = 1;
        for (auto res : *globalResults.data)
        {
            file << "Result " << i << ":" << endl;
            file << "File: " << res.file << endl;
            file << "Score: " << fixed << res.score << endl;
            file << "Summary: " << res.summary << endl;
            file << "###" << endl;
            i++;
        }

        file.close();
    }

    return 0;
}

vector<string> *tokenize_string(const string &line, const string &delim)
{
    auto tokens = new vector<string>();
    tokens->reserve(100);
    int found = 0;
    int start = 0;
    int end;
    while (found < line.size() && found != string::npos)
    {
        found = line.find(delim, start);
        end = found == string::npos ? line.size() : found;
        if (end - start > 0)
        {
            tokens->push_back(line.substr(start, end - start));
        }
        start = end + delim.size();
    }
    
    return tokens;
}

input_parameters get_input_params(const string &inputFile)
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

    // get the unique set of words from query vector
    sort(pQuery->begin(), pQuery->end());
    auto it = unique(pQuery->begin(), pQuery->end());
    pQuery->erase(it, pQuery->end());

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

pthread_t *create_start_thread(char name, const string &outputFile, int nResult, const vector<string> &query, shared_data<queue<string>> *abstracts, shared_data<vector<result>> *globalResults)
{
    pthread_t *thread = new pthread_t();
    thread_parameters *tParams = new thread_parameters{name, nResult, outputFile, query, abstracts, globalResults};
    int status = pthread_create(thread, NULL, (void *(*)(void *))thread_process, (void *)tParams);
    if (status != 0)
        return nullptr;
    return thread;
}

void *thread_process(thread_parameters *params)
{
    // extract parameters
    int nResult = params->nResult;
    auto abstracts = params->abstracts;
    auto globalResults = params->globalResults;

    // thread local results
    vector<result> localResults;
    localResults.reserve(nResult * 2);

    // take an abstract and process it
    while (true)
    {
        string abstract;
        {
            // acquire write lock and get the first abstract
            unique_lock lock(abstracts->_mutex);
            // break the loop if no abstract left in the queue
            if (abstracts->data->size() == 0)
                break;
            abstract = abstracts->data->front();
            abstracts->data->pop();
        }
        double minScore = localResults.size() < nResult ? -1 : localResults.back().score;
        result res = process_abstract(params->name, params->outputFile, abstract, params->query, minScore);
        if (res.score > minScore)
            insert_result(res, localResults, nResult);
    }

    // try to insert localResults to globalResults by comparing their scores
    {
        unique_lock lock(globalResults->_mutex);
        int index = 0;
        for (auto res : localResults)
        {
            index = insert_result(res, *globalResults->data, nResult, index);
            // if index is equal to nResult, it means the objects score is less than every element in the array
            // so don't process remaining items since their score is lower than the current one
            if (index >= nResult)
                break;
        }
    }

    return nullptr;
};

result process_abstract(const char name, const string &outputFile, const string &abstract, const vector<string> &query, double minScore)
{
    {
        ofstream file(outputFile, ios::app);
        file << "Thread " << name << " is calculating " << abstract << endl;
        file.close();
    }

    string text = "";
    {
        ifstream file("../abstracts/" + abstract);
        string line;
        while (getline(file, line))
        {
            text += line;
            if (text.back() != ' ')
                text += " ";
        }
        file.close();
    }

    double score = jaccard_score(text, query);
    string summary = "";
    // only calculate the summary if score is greater than min score of localResults
    if (score > minScore)
        summary = get_summary(text, query);
    return {abstract, score, summary};
}

double jaccard_score(const string &abstractText, const vector<string> &query)
{
    auto tokens = tokenize_string(abstractText, " ");
    unordered_set<string> uset;
    uset.reserve(tokens->size());
    for (string s : *tokens)
        uset.insert(s);

    // find number of intersecting elements
    int intersect = 0;
    for (string q : query)
        intersect += uset.count(q);

    // union is sum - intersection
    int _union = uset.size() + query.size() - intersect;

    return (double)intersect / (double)_union;
}

string get_summary(const string &abstractText, const vector<string> &query)
{
    unordered_set<string> querySet;
    querySet.reserve(query.size());
    for (auto s : query)
        querySet.insert(s);

    auto sentences = tokenize_string(abstractText, ".");

    vector<string> summarySentences;
    summarySentences.reserve(sentences->size());
    for (string sent : *sentences)
    {
        auto tokens = tokenize_string(sent, " ");
        for (string s : *tokens)
        {
            if (querySet.count(s))
            {
                summarySentences.push_back(sent);
                break;
            }
        }
    }

    string summary = "";
    for (string sent : summarySentences)
    {
        if (sent.front() == ' ')
            sent.erase(sent.begin());
        if (sent.back() != ' ')
            sent += " ";
        summary += (sent + ". ");
    }
    return summary;
}

int insert_result(result res, vector<result> &results, int nResult, int startIndex)
{
    if (results.size() >= nResult && res.score < results[nResult - 1].score)
        return nResult;

    vector<result>::iterator it;
    for (it = results.begin() + startIndex; it != results.end(); it++)
    {
        if (it->score < res.score)
            break;
    }

    int insertedIndex = it - results.begin();

    results.insert(it, res);
    if (results.size() > nResult)
        results.erase(results.begin() + nResult, results.end());
    
    return insertedIndex;
}