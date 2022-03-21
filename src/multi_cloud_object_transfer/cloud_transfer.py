# Cloud transfer, from S3 to Azure test

import boto3
import requests

url = 'https://s3.amazonaws.com/dev.control.gesdoc-cadena.com.co/ProceedingDocuments/TEST/ProcesosDisciplinarios/ProcesosDisciplinariosContraEmpleados/45678982342234231311232/C01PRUEBA/01QuejaDisciplinaria.PDF?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIA3FCI6BN43QTDHEQI%2F20220321%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20220321T053235Z&X-Amz-Expires=1800&X-Amz-SignedHeaders=host&X-Amz-Signature=d2ccedb30e3ddc7ab073ce81977ab542af402c38ec10952fe75e3df0f8688c25'

# Make the session
session = requests.Session()
# get the session in a data stream
response = requests.get(url, stream=True)

print(response)
with response as part:
    part.raise_for_status()
    with open("hola.pdf", "wb") as f:
        for chunk in part.iter_content(chunk_size=1024):
            f.write(chunk)
print("done")
