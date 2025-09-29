# -*- coding: utf-8 -*-
"""
Created on Sund Sept 28 2025

@author: Ilhuitemoc Jesús Martínez Santiago
"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
from pathlib import Path
import glob

def get_parquet_head(path):
    """
    Function made for reading and print basic information of any parquet files
    """    
    #--------------------------- tratamiento formato parquet ----------------------------
    parquet_table = pd.read_parquet(path)

    #------------------------------------ print data ------------------------------------
    #print(parquet_table.head())
    print(f"Dimensiones: {parquet_table.shape}")
    print(f"Columnas: {list(parquet_table.columns)}")
    print(parquet_table.iloc[1])

    #--------------------------- data save on parquet ----------------------------
    #---PARQUET-- We save the original base with a lib table object to easy manage.
    #pq.write_table(pa.Table.from_pandas(Base_FI_Siseco), os.path.join(ruta_fi, "Bases", "FI - Siseco - " + fecha_inicio + " a " +  fecha_fin) + ".parquet")
 
    return parquet_table

def get_parquet_base(path):
    """
    Function made for compiling various parquet files from nested directory structure
    Recursively searches all subdirectories for .parquet files and combines them
    Uses os.walk() - reliable method for hierarchical voting data structure
    """    
    
    # List to store all dataframes before concatenation
    data_tables = []
    files_processed = 0
    
    # Walk through all directories recursively
    for root, dirs, files in os.walk(path):
        # Filter only parquet files in current directory
        parquet_files = [f for f in files if f.endswith('.parquet')]
        
        for parquet_file in parquet_files:
            # Construct full file path
            file_path = os.path.join(root, parquet_file)
            
            try:
                # Read parquet file into dataframe
                df = pd.read_parquet(file_path)
                
                # Extract state and county from directory path structure
                path_parts = root.split(os.sep)
                for part in path_parts:
                    if part.startswith('state='):
                        df['state'] = part.replace('state=', '')
                    elif part.startswith('county_name='):
                        df['county'] = part.replace('county_name=', '').replace('%20', ' ')
                
                # Add source file tracking for debugging
                df['source_file'] = parquet_file
                
                # Add dataframe to collection
                data_tables.append(df)
                files_processed += 1
                
                print(f"Loaded: {file_path} (file process {files_processed})") #{df.shape[0]} rows, 

            except Exception as e:
                print(f"Error reading {file_path}: {e}")
                continue
    
    # Combine all dataframes into single dataset
    if data_tables:
        complete_base = pd.concat(data_tables, ignore_index=True)
        
        # Print consolidated data information
        print(f"Shape: {complete_base.shape}")
        #print(f"Columnas: {list(complete_base.columns)}")
        #print("Sample record:")
        #print(complete_base.iloc[1])
        
        return complete_base
    else:
        print("No parquet files     found in directory structure")
        return pd.DataFrame()

def main():

    #Datos de votación
    #votes = get_parquet_head('../bases/FI - Siseco - 201904 a 202408.parquet')

    base = get_parquet_base('../bases/voting_data')

if __name__ ==  '__main__':

    main()


""" def get_parquet_base(path):  
    
    #list of tables or df in the path, here we'll add all the information for gather later
    data_tables = []

    while document in path.os:

        table =                     #BLABLABLABA

    complete_base = pd.concat(data_tables, ignore_index=True)


    #------------------------------------ print data ------------------------------------
    #print(votes.head())
    print(f"Dimensiones: {complete_base.shape}")
    print(f"Columnas: {list(complete_base.columns)}")
    print(complete_base.iloc[1])

   
    return complete_base """
