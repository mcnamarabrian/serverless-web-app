import json
import uuid

import boto3


cf_stack_name = 'serverless-web-app'

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
cloudformation = boto3.client('cloudformation', region_name='us-east-1')

def get_quotes_table(stack_name):
    try:
        response = cloudformation.describe_stack_resources(
            StackName=stack_name
        )
    except Exception as e:
        print(f'Error: {str(e)}')
        raise()
    
    for s in response['StackResources']:
        if s['LogicalResourceId'] == 'QuotesTable':
            return s['PhysicalResourceId']

def load_quotes(quotes, table_name):
    table = dynamodb.Table(table_name)
    for q in quotes:
        q['id'] = str(uuid.uuid4())
        movie = q['movie']
        character_name = q['character_name']
        imdb_link = q['imdb_link']
        quote = q['quote']
        print(f'Adding a quote from {character_name} from the movie {movie}...')
        table.put_item(Item=q)

if __name__ == '__main__':
    quotes_table = get_quotes_table(cf_stack_name)

    with open('quotes.json') as json_file:
        quote_list = json.load(json_file)
    
    load_quotes(quote_list, quotes_table)
    
