import requests
from bs4 import BeautifulSoup
import boto3
import uuid

def lambda_handler(event, context):
    # URL donde está publicada la tabla de reportes sísmicos
    url = "https://ultimosismo.igp.gob.pe/ultimo-sismo/sismos-reportados"  # <--- reemplaza si es distinta

    response = requests.get(url)
    if response.status_code != 200:
        return {
            'statusCode': response.status_code,
            'body': 'Error al acceder a la página web'
        }

    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find('table')
    if not table:
        return {
            'statusCode': 404,
            'body': 'No se encontró la tabla en la página web'
        }

    rows = []
    for row in table.find_all('tr')[1:]:
        cells = row.find_all('td')
        if len(cells) < 5:
            continue
        reporte = cells[0].text.strip().replace('\n', ' ')
        referencia = cells[1].text.strip()
        fecha_hora = cells[2].text.strip()
        magnitud = cells[3].text.strip()
        enlace = cells[4].find('a')['href'] if cells[4].find('a') else None
        if enlace and not enlace.startswith('http'):
            enlace = f"https://www.igp.gob.pe{enlace}"

        rows.append({
            'Reporte': reporte,
            'Referencia': referencia,
            'FechaHora': fecha_hora,
            'Magnitud': magnitud,
            'EnlaceReporte': enlace
        })
    rows = rows[:10]

    # Guardar en DynamoDB
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('TablaWebScrapping')

    # Limpiar la tabla
    scan = table.scan()
    with table.batch_writer() as batch:
        for item in scan['Items']:
            batch.delete_item(Key={'id': item['id']})

    for i, row in enumerate(rows, 1):
        row['#'] = i
        row['id'] = str(uuid.uuid4())
        table.put_item(Item=row)

    return {
        'statusCode': 200,
        'body': rows
    }

