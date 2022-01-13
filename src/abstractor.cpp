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
#include <unordered_set>
#include <algorithm>
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
    const vector<string> query;
    safe_data<queue<string>> *abstracts;
    safe_data<vector<result>> *globalResults;
};

input_parameters get_input_params(string &inputFile);
pthread_t *create_start_thread(char name, const string &outputFile, int nResult, const vector<string> &query, safe_data<queue<string>> *abstracts, safe_data<vector<result>> *globalResults);
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

    auto params = get_input_params(inputFile);

    safe_data<queue<string>> abstracts;
    {
        unique_lock lock(abstracts._mutex);
        abstracts.data = params.abstracts;
    }

    safe_data<vector<result>> globalResults;
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

pthread_t *create_start_thread(char name, const string &outputFile, int nResult, const vector<string> &query, safe_data<queue<string>> *abstracts, safe_data<vector<result>> *globalResults)
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