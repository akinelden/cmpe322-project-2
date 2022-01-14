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
 *  globalResults : shared result vector which is sorted by score and 
 *                  contains best results from threads. Protected with mutex.
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
 * 5. Go back to step 2 until the abstract queue is emptied. 
 * 6. Compare the local results with global results and insert the ones with higher score (mutex required) 
 * 
 * @param params the thread parameters struct
 */
void *thread_process(thread_parameters *params);

/**
 * Function used to process an abstract file. All variables used in this function are 
 * local to the thread. Therefore there is no need for synchronization. 
 * Following actions are taken in the function: 
 * 1. Current thread and abstract file names are written to output file. 
 * 2. Content of the abstract file is read. 
 * 3. The Jaccard score of the content is calculated with jaccard_score function. 
 * 4. If the score is higher than the minimum score in the local results vector, 
 *      the summary is acquired with get_summary function added to result struct; 
 *      otherwise summary is ignored since the struct will not be included. 
 * 5. The result struct is returned 
 * 
 * @param name the name of the thread
 * @param outputFile the path of the output file
 * @param abstract the name of the abstract file
 * @param query the query vector
 * @param minScore the minimum score in the localResults vector
 * @return the result struct of the abstract
 */
result process_abstract(const char name, const string &outputFile, const string &abstract, const vector<string> &query, double minScore);

/**
 * Function used to calculate Jaccard score of a text using the query words. 
 * 
 * @param abstractText the text of the abstract
 * @param query the query vector
 * @return the Jaccard score as double
 */
double jaccard_score(const string &abstractText, const vector<string> &query);

/**
 * Function used to get sentences which contains a word from query. 
 * Result is a single string which is concatenation of sentences.
 * 
 * @param abstractText the text of the abstract
 * @param query the query vector
 * @return the summary string
 */
string get_summary(const string &abstractText, const vector<string> &query);

/**
 * Function used to insert a result to a sorted result vector without breaking 
 * the ordering. If the new result's score is smaller than other results in the vector 
 * and vector contains at least nResult number of structs, then it's not inserted. 
 * To increase the search efficiency, start index of the search can be provided 
 * if a prior knowledge about the vector exists.
 * 
 * @param res the result to be inserted
 * @param nResult the number of results provided by the input file
 * @param startIndex optional start index, if not provided starts from searching at index 0
 * @return the index at which result is inserted
 */
int insert_result(result res, vector<result> &results, int nResult, int startIndex = 0);

/**
 * Function used to tokenize a string. Empty tokens are discarded.
 * 
 * @param line the string to be tokenized
 * @param delim the delimiter string
 * @return the token vector
 */
vector<string> *tokenize_string(const string &line, const string &delim);

int main(int argc, char *argv[])
{
    // input file and output file parameters are expected from user
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
        // if the output file exists, open it and write empty string
        // to clear its content.
        ofstream file(outputFile, ios::out);
        file << "";
        file.close();
    }

    // input parameters are acquired
    auto params = get_input_params(inputFile);

    // the abstract list is put into a shared_data struct
    // since it will be shared between threads
    shared_data<queue<string>> abstracts;
    {
        // unique lock is used since data manipulation is done
        unique_lock lock(abstracts._mutex);
        abstracts.data = params.abstracts;
    }

    // a global result vector is used to store best N results.
    // the vector is initialized as empty, and it's shared with all threads.
    // therefore the vector is put in shared_data struct and protected with mutex.
    // when a thread finishes processing abstracts, it compares its local results
    // with the global results. if a local result has a higher score than any 
    // global result, it's added to globalResults vector and the global result
    // with smallest score is removed from the vector. For easier handling, 
    // global result vector is always maintained in descending order.
    shared_data<vector<result>> globalResults;
    {
        unique_lock lock(globalResults._mutex);
        vector<result> *results = new vector<result>();
        results->reserve(params.nResult * 2);
        globalResults.data = results;
    }

    // the vector which contains all created threads
    vector<pthread_t *> threads;
    threads.reserve(params.nThread);
    char letter = 'A';
    // create T number of threads
    for (int i = 0; i < params.nThread; i++)
    {
        // create threads by providing necessary parameters
        pthread_t *pThread = create_start_thread(letter + i, outputFile, params.nResult, *params.query, &abstracts, &globalResults);
        // add created thread to vector
        if (pThread)
            threads.push_back(pThread);
        else
        {
            // if the thread cannot be created, write to stdout and exit
            cout << "Couldn't create thread : " << letter + i << endl;
            exit(1);
        }
    }

    // wait for each thread to finish its job
    for (auto pThread : threads)
    {
        pthread_join(*pThread, NULL);
    }

    {
        // open the output file and set the double precision to 4
        ofstream file(outputFile, ios::app);
        file.precision(4);
        file << "###" << endl;
        
        // each thread checks the globalResults and adds its best
        // results if they are higher than the existing ones.
        // and each thread makes this action while preserving the
        // descending order in the vector. the first result in the
        // vector is the one with highest score, and the last one 
        // is the one with smallest score.
        // therefore we don't need to manipulate the globalResults 
        // vector, it's enough to read the results one by one
        // and write to the output file.
        shared_lock lock(globalResults._mutex);
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

        // close the output file
        file.close();
    }

    // delete allocated variables
    for (auto t : threads)
        delete t;
    {
        unique_lock lock(globalResults._mutex);
        delete globalResults.data;
    }
    {
        unique_lock lock(abstracts._mutex);
        delete abstracts.data;
    }

    // successfully return
    return 0;
}

vector<string> *tokenize_string(const string &line, const string &delim)
{
    // allocate a vector for tokens
    auto tokens = new vector<string>();
    tokens->reserve(100);
    // the index at which delimiter is found
    int found = 0;
    // the index to start searching for delimiter
    int start = 0;
    // the index at which the found token ends
    int end;
    // run the loop, if found is equal to string::npos then search is finished
    while (found < line.size() && found != string::npos)
    {
        // find the delimiter in the string starting from start index
        found = line.find(delim, start);
        // if found is equal to npos, end of the token is equal to size of line,
        // otherwise end is equal to found
        end = found == string::npos ? line.size() : found;
        // if end is equal to start, it means token is empty (two consecutive delimiters)
        // otherwise add the token to tokens vector
        if (end - start > 0)
        {
            tokens->push_back(line.substr(start, end - start));
        }
        // the start of the next token is end of the current token + size of the delimiter
        start = end + delim.size();
    }
    
    return tokens;
}

input_parameters get_input_params(const string &inputFile)
{
    // open the input file
    ifstream file(inputFile);
    if (!file)
    {
        cout << "Cannot open the file : " << inputFile << endl;
        exit(1);
    }

    // first line, integer parameters (T, A, N)
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

    // initialize a FIFO queue for abstracts
    queue<string> *pAbstracts = new queue<string>();
    // read following A number of lines
    int i = 0;
    while (getline(file, line) && i++ <= A)
    {
        // push each abstract name to queue
        pAbstracts->push(line);
    }

    // create an input_parameters struct using the variables
    input_parameters params = {T, A, N, pQuery, pAbstracts};
    return params;
}

pthread_t *create_start_thread(char name, const string &outputFile, int nResult, const vector<string> &query, shared_data<queue<string>> *abstracts, shared_data<vector<result>> *globalResults)
{
    // initialize a thread variable
    pthread_t *thread = new pthread_t();
    // create thread_parameters struct using given parameters
    thread_parameters *tParams = new thread_parameters{name, nResult, outputFile, query, abstracts, globalResults};
    // create the thread which executes the thread_process function using tParams parameter
    int status = pthread_create(thread, NULL, (void *(*)(void *))thread_process, (void *)tParams);
    // if status is not 0, thread couldn't be created, return nullptr
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

    // thread processes abstracts one by one,
    // puts the results into localResults vector in
    // descending order. when processing abstracts is done
    // (abstract queue is emptied), globalResults is compared
    // with localResults. if localResults contain a result
    // with higher score, than it's inserted and smallest global result
    // (the last element) is removed. localResults is initialized here
    vector<result> localResults;
    localResults.reserve(nResult * 2);

    // take an abstract from the queue and process it until the queue is emptied
    while (true)
    {
        string abstract;
        {
            // we are popping from queue, therefore acquire write lock
            // and get the first abstract
            unique_lock lock(abstracts->_mutex);
            // break the loop if no abstract left in the queue
            if (abstracts->data->size() == 0)
                break;
            abstract = abstracts->data->front();
            abstracts->data->pop();
        }
        // if localResults vector contains less than nResult object, then next
        // processed abstract will be added to vector without considering its score.
        // therefore set minScore to -1 if localResult contains less than nResult,
        // otherwise set minScore to last element of localResults which has the smallest score
        // since this is a sorted vector
        double minScore = localResults.size() < nResult ? -1 : localResults.back().score;

        // process the abstract: read file, calculate jaccard score and summary
        result res = process_abstract(params->name, params->outputFile, abstract, params->query, minScore);

        // if score of the new result is higher than minScore, 
        // insert the new result to localResults vector
        // note that insert_result function scans the vector and inserts the
        // result without breaking descending order
        if (res.score > minScore)
            insert_result(res, localResults, nResult);
    }

    // abstract process is finished, therefore try to insert local results to global results
    {
        // we are changing the globalResults vector, therefore acquire write lock
        unique_lock lock(globalResults->_mutex);
        // create a variable for previously inserted result index
        int index = 0;

        // iterate local results from first to last element (which means 
        // highest score to lowest score)
        for (auto res : localResults)
        {
            // try to insert the current local result to global results vector.
            // insert_result function scans the vector starting from index
            // when the first element which has smaller score than the new result is found,
            // new result is inserted in that position and the last object (smallest score) 
            // is removed from the vector. the insert position is returned from the function.
            // therefore for the next local result we can start scanning vector from that position
            // since the next local result's score is always smaller than the previous one
            index = insert_result(res, *globalResults->data, nResult, index);
            // if index is equal to nResult, it means the objects score is less than every element in the array
            // so don't process remaining local items since their score is lower than the current one
            if (index >= nResult)
                break;
        }
    }

    // cleanup the thread_parameters struct
    delete params;

    return nullptr;
};

result process_abstract(const char name, const string &outputFile, const string &abstract, const vector<string> &query, double minScore)
{
    {
        // open the output file and write the thread and abstract name
        ofstream file(outputFile, ios::app);
        file << "Thread " << name << " is calculating " << abstract << endl;
        file.close();
    }

    // initialize an emptry string to hold content of abstract
    string text = "";
    {
        // open the abstract file
        ifstream file("../abstracts/" + abstract);
        string line;
        // read the abstract file line by line
        while (getline(file, line))
        {
            // add each line to text, if the line doesn't end with space, add a space
            text += line;
            if (text.back() != ' ')
                text += " ";
        }
        file.close();
    }

    // calculate the jaccard score
    double score = jaccard_score(text, query);

    string summary = "";
    // if the score is not greater than min score, no need to find the summary
    // since the result will not be added to local results vector.
    // otherwise find the summary
    if (score > minScore)
        summary = get_summary(text, query);
    return {abstract, score, summary};
}

double jaccard_score(const string &abstractText, const vector<string> &query)
{
    // first tokenize the whole abstract text by space and get the words
    auto tokens = tokenize_string(abstractText, " ");
    // use an unordered set to store the tokens since it discards duplicate words
    unordered_set<string> uset;
    uset.reserve(tokens->size());

    // insert each token to uset, if token already exist in the set, 
    // it's simply discarded by the set
    for (string s : *tokens)
        uset.insert(s);

    // find number of intersecting words by counting each query word in the set
    int intersect = 0;
    for (string q : query)
        intersect += uset.count(q);

    // union is sum - intersection
    int _union = uset.size() + query.size() - intersect;

    // clean the tokens vector
    delete tokens;

    // return the insersection / union as jaccard score
    return (double)intersect / (double)_union;
}

string get_summary(const string &abstractText, const vector<string> &query)
{
    // create a query set since set structure provides
    // easier search
    unordered_set<string> querySet;
    querySet.reserve(query.size());
    for (auto s : query)
        querySet.insert(s);

    // get each sentence in the text by using dot (.) as delimiter
    auto sentences = tokenize_string(abstractText, ".");

    vector<string> summarySentences;
    summarySentences.reserve(sentences->size());
    // iterate each sentence
    for (string sent : *sentences)
    {
        // find words of sentence
        auto tokens = tokenize_string(sent, " ");

        // iterate each word and check if it exists in the query set
        for (string s : *tokens)
        {
            // if query set contains that word, 
            // add sentence to summary vector and break the loop
            if (querySet.count(s))
            {
                summarySentences.push_back(sent);
                break;
            }
        }

        // cleanup tokens vector
        delete tokens;
    }

    // put each sentence next to other and separate with dots (.)
    string summary = "";
    for (string sent : summarySentences)
    {
        if (sent.front() == ' ')
            sent.erase(sent.begin());
        if (sent.back() != ' ')
            sent += " ";
        summary += (sent + ". ");
    }

    // cleanup the sentences vector
    delete sentences;

    return summary;
}

int insert_result(result res, vector<result> &results, int nResult, int startIndex)
{
    // if results vector contain at least nResult items and 
    // the score of nResult'h item is higher than the new result
    // then new result is not inserted and nResult is returned as the index
    if (results.size() >= nResult && res.score < results[nResult - 1].score)
        return nResult;

    // starting from the begin + startIndex, iterate each item and find the first one
    // with lower score than the new one
    vector<result>::iterator it;
    for (it = results.begin() + startIndex; it != results.end(); it++)
    {
        if (it->score < res.score)
            break;
    }

    // calculate the insertion index
    int insertedIndex = it - results.begin();

    // insert the new result to that index
    results.insert(it, res);
    // if results vector has more than nResult item, delete excessive ones
    if (results.size() > nResult)
        results.erase(results.begin() + nResult, results.end());
    
    // return the inserted index
    return insertedIndex;
}