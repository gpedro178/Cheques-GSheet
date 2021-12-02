###################################
#
#     Google Sheet Updater
#      From SQL to GSheet
#            Cheques
#
#        26/11/21 - 02/12/21
###################################


import os
import sys
import pathlib
import time

# Allow imports from the top folder
#sys.path.insert(0,str(pathlib.Path(__file__).parent.parent))


import json
import pandas as pd
from apscheduler.schedulers.background import BackgroundScheduler
from openpyxl import load_workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.styles import NamedStyle
from openpyxl.styles import Font, Fill
from googleapiclient.discovery import build
from google.oauth2 import service_account
import logging


from DatosLogin import login, googleSheet_cheques
from Conectores import conectorMSSQL



logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        , level=logging.INFO
)
logger = logging.getLogger(__name__)


# Path and name of control file
ubic = str(pathlib.Path(__file__).parent) + "\\"
nombreExcel = "Cheques_Control.xlsx"


def _new_Data():
    '''
    This function will extract data from a SQL Server db to a Dataframe
    then if there is no "Cheques_Control.xlsx" file, its going to create
    it and will return the complete Dataframe.
    If "Cheques_Control.xlsx" exist then its going to compare the Dataframe to
    the data in the .xlsx file, if there are new rows, its going to update the
    .xlsx file and return a dataframe with new rows only.
    '''

    # TODO: Replace comparison with "Cheques_Control.xlsx" with a comparison
    # made with rows extracted from the Google Sheet to avoid duplicates
    # due to possible errors when creating the .xlsx file

    conexMSSQL = conectorMSSQL(login)

    df_cheques = pd.read_sql(
        """
        SET NOCOUNT ON --Needed for Pandas query due to temp list @lista

        DECLARE @fecha as date
        SET @fecha = DATEADD(DAY,-10,CAST(getdate() as date))
    
        SELECT 
            RTRIM(CR.[UEN]) AS 'UEN'
            ,CONCAT(CR.PTOVTAREC,'--',CR.NRORECIBO) as 'NRORECIBO'
            ,CAST(RV.NROCLIENTE as nvarchar) AS 'NROCLIENTE'
            ,RTRIM(FC.NOMBRE) AS 'NOMBRE'
            ,RTRIM(CR.[BANCO]) AS 'BANCO'
            ,CAST(CR.[NROCHEQUE] as nvarchar) AS 'NROCHEQUE'
            ,CR.[IMPORTE]
            ,CAST(CR.[FECHAVTOSQL] as date) AS 'FECHA VENCIMIENTO'
            ,RTRIM(V.NOMBREVEND) AS 'VENDEDOR'
            ,RTRIM(RV.USUARIO) AS 'USUARIO SGES'
            ,CAST(RV.FECHASQL as smalldatetime) AS 'FECHA INGRESO'
            ,CONVERT(nvarchar(66),HASHBYTES('SHA2_256'
                ,CONCAT(
                    RTRIM(CR.[UEN])
                    ,CONCAT(CR.PTOVTAREC,'--',CR.NRORECIBO)
                    ,CAST(RV.NROCLIENTE as nvarchar)
                    ,RTRIM(FC.NOMBRE)
                    ,RTRIM(CR.[BANCO])
                    ,CAST(CR.[NROCHEQUE] as nvarchar)
                    ,CR.[IMPORTE]
                    ,CAST(CR.[FECHAVTOSQL] as date)
                    ,RTRIM(V.NOMBREVEND)
                    ,RTRIM(RV.USUARIO)
                    ,CAST(RV.FECHASQL as smalldatetime)
                )
            ),1)  as 'HASH'
        FROM [Rumaos].[dbo].[CCRec02] AS CR
        join Rumaos.dbo.RecVenta AS RV 
            ON CR.UEN = RV.UEN
            AND CR.PTOVTAREC = RV.PTOVTA
            AND CR.NRORECIBO = RV.NRORECIBO
        join Rumaos.dbo.FacCli as FC
            ON RV.NROCLIENTE = FC.NROCLIPRO
        left join Rumaos.dbo.Vendedores as V
            ON FC.NROVEND = V.NROVEND
        where (CR.mediopago = 4 OR CR.nrocheque <> 0)
        and RV.FECHASQL >= '20211202'
        order by CR.UEN,RV.FECHASQL
        """
        ,conexMSSQL
    )
    df_cheques = df_cheques.convert_dtypes()
    
    # print(df_cheques.info())
    # print(df_cheques.head())
    

    # Check if we have the control file
    if os.path.exists(ubic + nombreExcel):

        # Read the control file as a DataFrame and modify column types
        # of "NROCLIENTE" and "NROCHEQUE" to str
        df_control = pd.read_excel(ubic + nombreExcel
            , sheet_name="Cheques"
            , dtype={
                "NROCLIENTE":str
                , "NROCHEQUE":str                
            }
        )
       
                
        # Concatenate control DF with the newly extracted DF
        merged = pd.concat([
            df_control.reset_index()
            , df_cheques.reset_index()
        ])

        # Remove column "index"
        merged = merged.drop(columns=["index"])
        
        # Drop all duplicates using the "HASH" column
        df_newRows = merged.drop_duplicates(["HASH"], keep=False)        
        
        # Replace NaN with ""
        df_newRows = df_newRows.fillna("")
               
        
        # If there are no new rows return empty DataFrame
        if len(df_newRows.index) == 0:
            return df_newRows

        # If we have rows...    
        else:
            # update .xlsx file with new rows...
            
            # Load the control file
            wBook = load_workbook(ubic + nombreExcel)

            # Get the active sheet
            wSheet = wBook.active

            # Append every row to the end of the file
            for row in dataframe_to_rows(df_newRows, index=False, header=False):
                wSheet.append(row)

            # Get column "FECHA VENCIMIENTO"
            col = wSheet["H"]

            # Change format of date of the whole column
            for cell in col:
                cell.number_format="yyyy-mm-dd"

            # Save and close the file
            wBook.save(ubic + nombreExcel)
            wBook.close()

            # and return df with the new rows
            return df_newRows


    # If we dont have the control file        
    else:
        # Create the control file with the newly extracted DF
        
        df_cheques.to_excel(
            ubic + nombreExcel
            , sheet_name="Cheques"
            , header=True # Use column names as headers
            , index=False
            , na_rep=""
            , engine="openpyxl"
        )

        return df_cheques





def _write_sheet(df:pd.DataFrame):
    '''
    This function will receive a dataframe and will insert its rows into the
    predefined Google Sheet in the predefined variable "googleSheet_cheques"
    '''

    # Scopes will limit what we can do with the sheet
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    SERVICE_ACCOUNT_FILE = ".\quickstart.json"

    # Credentials and service for the Sheets API
    creds = service_account.Credentials.from_service_account_file(
                SERVICE_ACCOUNT_FILE, scopes=SCOPES)

    service = build('sheets', 'v4', credentials=creds)

    # Call the Sheets API
    sheet = service.spreadsheets()


    # Check for non empty DataFrame
    if len(df.index) > 0:
        # To input data into a Google sheet we need to transform it 
        # into an array
        dfHeaders = df.columns.values.tolist()

        # Getting the headers array
        dfHeadersArray = [dfHeaders]

        dfData = df.values.tolist()

        # The Google API library will transform the values list to a JSON but 
        # this trigger a "TypeError: Object of type date is not JSON
        # serializable" due to datetime objects.        
        
        # To fix the error we transform the list into a json using
        # "default=str" to get all the dates like a string.
        dfData = json.dumps(dfData, default=str)

        # Using the Json as an input to write the sheet will raise
        # an "Invalid Value" error so we transform it again to a Dataframe
        dfData = pd.read_json(dfData)

        # And then to a list again ready to be written on the Google Sheet
        dfData = dfData.values.tolist()

# TODO: fix duplicate dropping
#####################################################################
        raise SystemExit()
#####################################################################


        # Values and how to load them in the sheet
        value_range_body = {
            "majorDimension": "ROWS", # Write data in rows instead of columns
            "values": dfData
        }

        # Make a request to append data in the selected sheet and range
        request = sheet.values().append(
            spreadsheetId=googleSheet_cheques
            , range="Hoja 1!A2:L" # This avoid the headers row
            , valueInputOption="USER_ENTERED"
            , body=value_range_body)

        # Run the request
        request.execute()

        # Show how many rows were inserted
        logger.info(str(len(df.index)) + " NEW ROWS INSERTED")

    # If DataFrame is empty...
    elif len(df.index) == 0:
        logger.info("NO NEW ROWS")


def main():
    
    # Create function to be called for scheduler job
    def _for_job():
        _write_sheet(_new_Data())
        return
    _for_job()
    # # Create background scheduler
    # sched = BackgroundScheduler(daemon=True)

    # # Add job to scheduler and define interval to be executed
    # sched.add_job(
    #     _for_job
    #     , name="GSheet_updater"
    #     , trigger="interval"
    #     , seconds=30 
    # )

    # # Start Scheduler, can be stopped with Ctrl-C
    # sched.start()

    # # Keeping alive thread for background scheduler
    # try:
    #     while True:
    #         time.sleep(30)
    # except:
    #     sched.shutdown()



if __name__ == '__main__':
    main()

    

