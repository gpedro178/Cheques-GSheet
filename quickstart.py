
from googleapiclient.discovery import build
from google.oauth2 import service_account


import os
import sys
import pathlib

# Allow imports from the top folder
sys.path.insert(0,str(pathlib.Path(__file__).parent.parent))

import json
import pandas as pd

from DatosLogin import login, googleSheet_cheques
from Conectores import conectorMSSQL





conexMSSQL = conectorMSSQL(login)

df_cheques = pd.read_sql(
    """
    SET NOCOUNT ON --Needed for Pandas query due to temp list @lista

    DECLARE @fecha as smalldatetime
    -- Filtering by datetime >= @fecha will go from 8 AM of previous day
    -- to the time of the report
    IF DATENAME(WEEKDAY,GETDATE()) = 'lunes'
        SET @fecha = dateadd(HOUR,8,cast(dateadd(DAY,-3,cast(getdate() as date)) as datetime))
    ELSE
        SET @fecha = dateadd(HOUR,8,cast(dateadd(DAY,-1,cast(getdate() as date)) as datetime))

    SELECT
        RTRIM(CR.[UEN]) AS 'UEN'
        ,CAST(CR.[NRORECIBO] as nvarchar) AS 'NRORECIBO'
        ,CAST(RV.NROCLIENTE as nvarchar) AS 'NROCLIENTE'
        ,RTRIM(FC.NOMBRE) AS 'NOMBRE'
        ,RTRIM(CR.[BANCO]) AS 'BANCO'
        ,CAST(CR.[NROCHEQUE] as nvarchar) AS 'NROCHEQUE'
        ,CR.[IMPORTE]
        ,CAST(CR.[FECHAVTOSQL] as date) AS 'FECHA VENCIMIENTO'
        ,RTRIM(V.NOMBREVEND) AS 'VENDEDOR'
        ,RTRIM(RV.USUARIO) AS 'USUARIO SGES'
        ,CAST(RV.FECHASQL as smalldatetime) AS 'FECHA INGRESO'
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
    and RV.FECHASQL >= @fecha
    order by CR.UEN,RV.FECHASQL
    """
        ,conexMSSQL)
df_cheques = df_cheques.convert_dtypes()

# To input data into a Google sheet we need to transform it into an array
dfHeaders = df_cheques.columns.values.tolist()

# Getting the headers array
dfHeadersArray = [dfHeaders]

# The Google API library transform the values list to a JSON but this trigger a
# "TypeError: Object of type date is not JSON serializable" due to datetime 
# objects
dfData = df_cheques.values.tolist()

# To fix the error we transform the list into a json using "default=str"
# to get all the dates like a string
dfData = json.dumps(dfData, default=str)

# Using the Json as an input to write the sheet will raise an "Invalid Value" 
# error so we transform it again to a Dataframe
dfData = pd.read_json(dfData)

# And then to a list again ready to be write on the Google Sheet
dfData = dfData.values.tolist()

# print(dfHeaders)
# print(dfData)

# Scopes will limit what we can do with the sheet
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SERVICE_ACCOUNT_FILE = ".\quickstart.json"

# The ID of a sample spreadsheet.
SAMPLE_SPREADSHEET_ID = googleSheet_cheques


def main():
    '''
    
    '''

    creds = service_account.Credentials.from_service_account_file(
                SERVICE_ACCOUNT_FILE, scopes=SCOPES)

#InfoCheques\quickstart-static-map-333315-7bf4b80337d0.json
    service = build('sheets', 'v4', credentials=creds)

    # Call the Sheets API
    sheet = service.spreadsheets()


    # Input data will be interpreted as typed by the user.
    value_input_option = 'USER_ENTERED' 

    value_range_body = {
        "majorDimension": "ROWS",
        "values": dfData
    }

    request = service.spreadsheets().values().append(
        spreadsheetId=SAMPLE_SPREADSHEET_ID
        , range="Hoja 1!A2:Z"
        , valueInputOption=value_input_option
        , body=value_range_body)

   

    request.execute()




if __name__ == '__main__':
    main()