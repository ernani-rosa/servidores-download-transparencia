import pandas as pd
import asyncio
import aiohttp
import time
import joblib
from key import key
import os

ASYNC_AGENCIES = 3
ASYNC_PAGES = 25

async def get_agency(code):
    '''
    Takes a code from a "Orgão do SIAPE" and stores a raw list of all employees data as a pickle at ./pickles/{code}.pkl.
    '''
    
    #setup the API call
    base_url = "https://api.portaldatransparencia.gov.br/api-de-dados/"
    endpoint = "servidores"
    params = "?orgaoServidorExercicio={}&pagina={}"
    url = base_url + endpoint + params
    headers = {"chave-api-dados":key()}

    #chunk size will determine the number of simultaneous async calls,
    # for agencies and employee pages
    #ASYNC_PAGES = 20

    #start page index and empty list of employees
    page_index = 1
    employees = []
    
    #gets the first page of the agency employees
    async with aiohttp.ClientSession() as session:
        r = await session.get(url.format(code,page_index),headers=headers)

    if r.status != 200:
        print("Status code: {}.".format(r.status))
        return

    try:
        emp_page = await asyncio.wait_for(r.json(),timeout=60)
    except asyncio.TimeoutError:
        print(f"Code {code} timed out!")
        return

    
    #if it has less than 15 employees, append them to the list and return.
    if len(emp_page)<15:

        employees += emp_page
        print(f"Code {code} finished, dumping...",end='')
        joblib.dump(employees,f"./pickles/{code}.pkl",compress=3)
        print(f"Done")

    #else, enters while loop
    else:
        while True:
            
            page_index += ASYNC_PAGES
            starting_page = page_index-ASYNC_PAGES+1

            tasks = []
            new_employees = []
            

            print("{} - getting pages {}-{}".format(code,starting_page,page_index))
            # adds next requests to tasks, getting 10 more pages
            async with aiohttp.ClientSession() as session:
                for page in range(starting_page,page_index+1):
                    tasks.append(asyncio.create_task(session.get(url.format(code,page),
                                                                 headers=headers)))
                    
                #waits for all the tasks
                response_list = await asyncio.gather(*tasks)                
                
                #grabs the list of employees in each page
                for page in response_list:
                    if page.status == 200:
                        try:
                            new_employees += await asyncio.wait_for(page.json(),timeout=20)
                        except asyncio.TimeoutError:
                            print("Code {code} timed out!")
                            return
                    else:
                        print(f"Status code:{page.status}")
                        return
                    
            #appends new employee list to list to be returned
            employees += new_employees

            #sleeps 0 to release loop
            await asyncio.sleep(0.1)
            
            #if the new list is smaller than 15 times the chunk size, we got all the employees, so return
            if len(new_employees)<15*ASYNC_PAGES:
                #return code,employees
                print(f"Code {code} finished, dumping...",end='')
                joblib.dump(employees,f"./pickles/{code}.pkl",compress=3)
                print(f"Done")
                break #finishes the function
            else:
                #else, continues to the next chunck of the lists
                pass                       
            
            

async def call_agencies(codes,sim_calls):
    '''
    Takes a list of codes and a number simultaneous calls. Returns 1 if all codes are already downloaded.
    '''


    #check if pickle folder exists; otherwise creates one
    if not os.path.exists(r'./pickles/'):
        os.makedirs(r'./pickles/')

    #First, filter the list to check which pickle I already have, create new list of the missing ones
    missing_codes = []
    for code in codes:
        if not os.path.exists(rf'./pickles/{code}.pkl'):
            print(f"Code {code} pickle missing.")
            missing_codes.append(code)
        else:
            print(f"Code {code} pickle found, skipping...")

    if len(missing_codes) == 0:
        return 1


    print(f"Missing {len(missing_codes)} codes.")

    #Creates semaphore
    sema = asyncio.Semaphore(value=sim_calls)

    tasks = [asyncio.create_task(worker(sema,code)) for code in missing_codes]
    await asyncio.gather(*tasks)

    return 0


async def worker(sema,code):

    await sema.acquire()
    print(f"Semaphore aquired for {code}!")
    await get_agency(code)
    print(f"Releasing {code} semaphore.")
    sema.release()


async def main() -> None:

    #Load agencies
    agencies_df = pd.read_csv("orgaos_siape.csv")
    

    while True:
        complete = await call_agencies(agencies_df['codigo'],ASYNC_AGENCIES)

        if complete:
            print("All pickles saved.")
            break

        if not complete:
            print("Loop complete, still some files missing - sleeping for 5 minutes and trying again.")
            time.sleep(300)

    
if __name__ =="__main__":
    asyncio.run(main())




