#%%
import pandas as pd
from bs4 import BeautifulSoup
import requests
import os.path as op
from os import makedirs

# %%
def pull_nhanes(begin_year, sub_dirs, outdir=None):

    '''
    This function can be used to read and save data from NHANES. 
    Just input a starting year, a list of subfolders and a valid directory.

    Parameters:
    -----------
    
    begin_year(int): starting year of interest (e.g. in 2015-2016 -> 2015).
    outdir(str): Path to the directory in which you wish to save the data
    sub_dirs(list): List of subdirectories to scrape (e.g. ['Examination', ..]).
    '''

    #Consts
    NHANES_BASE = 'https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component='
    DATABASE = 'https://wwwn.cdc.gov/' 

    year_of_interest = '&CycleBeginYear=' + str(begin_year)

    #create an outdir if it doesnt already exist
    outdir = op.join(outdir, str(begin_year))
    if outdir is not None and not op.isdir(outdir):
        makedirs(outdir)

    #if outdir is None dont allow to loop over multiple subdirs
    if outdir is None and len(sub_dirs) > 1:
        raise Exception('You need to specify an outdir when you want to look at data from multiple sub directories')

    all_dfs = []

    for cur_sub_dir in sub_dirs:

        page = requests.get(NHANES_BASE + cur_sub_dir + year_of_interest)

        soup = BeautifulSoup(page.text, 'html.parser')

        all_tags = soup.find_all('tr')
        my_tags = []

        for tag in all_tags:
            anchors = tag.find_all('a')
            if len(anchors) > 0:
                my_tags.append(anchors[-1]['href'][1:])

        for tag in my_tags:
            try:
                cur_df = pd.read_sas(op.join(DATABASE, tag))
            except ValueError:
                print(f'Couldnt read sas file for {tag}')

            if outdir is not None:
                fname2save =  op.join(outdir, tag.split('/')[-1][:-3] + 'csv')
                cur_df.to_csv(fname2save)

            else:
                all_dfs.append(cur_df)

    if outdir is None:
        for idx, f in enumerate(all_dfs):
            if idx == 0: 
                df = f
            else:
                df2merge = f
                df = df.merge(df2merge, on='SEQN', how='outer')

        return df

#%%
def merge_datasets(indir, data_files):
    '''
    This function takes a set of datafiles from nhanes and merges them based on "SEQN".

    Parameters:
    -----------

    indir(str): directory of the nhanes data stored on your system
    data_files(list): list containing the names of the stored ".csv" files (e.g. ['AUX_I', 'AUQ_I', 'DEMO_I', ..])  
    '''

    for idx, f in enumerate(data_files):
        if idx == 0: 
            df = pd.read_csv(op.join(indir, f + '.csv'))
        else:
            df2merge = pd.read_csv(op.join(indir, f + '.csv'))
            df = df.merge(df2merge, on='SEQN', how='outer')

    return df