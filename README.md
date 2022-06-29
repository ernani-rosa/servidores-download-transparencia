# Introduction

This is a script I wrote to download employee data from the API of Portal da Transparencia (https://api.portaldatransparencia.gov.br/).

It uses asynchronous corroutines with asyncio to make simultaneous API requests and save the result of each list of employees from an agency a pickle using joblib.

### Challenges
- Using asynchronous code to speed up the data acquiring process
- Dealing with the API itself:
	- Sometimes it just stops responding;
	- Response speed drastically varies with time of the day;
	- There is(?) a limit of 400 (700 between 0h-6h) calls per minute;
- Controlling the speed of execution to ensure some progress before the API stops responding and/or the limit number of calls is reached.

### Results
The script runs using a key.py file with a function that only returns a string with the API key. You can have your own key by following the steps shown here: https://www.portaldatransparencia.gov.br/api-de-dados/cadastrar-email

It uses a list of agencies (órgãos), that was acquired using the same API. The list is already in .csv format in this repository. Then, for each agency it tries to get all the employees data.  
An `asyncio.Semaphore` is used to manage the number of simultaneous agencies it is calling for the employees. Once an agency list of employees is finished, it stores it's contents in a pickle file inside the `./pickle` folder. A timeout is needed, as sometimes the API simply stops responding. When that happens, the script simply moves to the next agency code, leaving the timed-out one for the next iteration. A code 429 is expected, and the script simply stops making calls for the entire iteration, and sleeps for some time before attempting the whole list again. It checks for the pickle files it already has downloaded and skips them.

This ended up being a rough looking solution, but it gets the job done. Some agencies have too many employees, meaning it can reach the point of a 429 code before it finishes downloading the data, but results vary depending on the speed the API is responding and the time of the day. I tried to control the number of calls by counting and using time.sleep to wait 60 seconds before I reach the maximum number of calls, but this created some problems with the execution of the event loop, and I ended up giving this solution up for now, until I have a better understanding of how asyncio manages the coroutines. I could use asyncio.sleep, but this function only pauses the current coroutine, creating unwanted behavior when using many concurrent coroutines and a global connection counter.

### How to use
Set up your API key by creating a python script with the `key` function, that returns your API key as a string.
Run async_api_download.py to run the code. You can adjust the global variables `ASYNC_AGENCIES` and `ASYNC_PAGES` as you like - higher number of pages allows faster download of an agency with a high number of employees, but has the drawback of "wasting" calls on empty pages (as the script only knows if the last page has been reached after it awaits for all the page tasks to finish). A higher number of async agencies can increase the speed in which the script downloads agencies with small number of employees, but at the risk of reaching the maximum number of connections before it can dump all the employees to a file. You can leave the code running on the background.

After you finish downloading all the data, the employee_parser.py script can be used to parse all the employees into a single csv file.
The result is a 1million~ rows csv. The desired columns can be controlled by editing the script.