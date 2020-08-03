import json
import os
import uuid

from aws_lambda_powertools import Logger
import boto3


aws_region = os.getenv('AWS_REGION')

table_name = os.getenv('QUOTES_TABLE')

dynamodb = boto3.client('dynamodb', region_name=aws_region).Table(table_name)

logger = Logger()


def post_quotes(movie_quotes):
    """Look for quotes based on movie name or character

    Parameters
    ----------
    movie_quotes: list
        Movie quotes to add
    
    Returns
    -------
    response: dict
        Returns response of post operation
    """
    if len(movie_quotes)
        logger.error('Need to include a list of movie quotes')
    
    for m in movies:
        try:
            id = str(uuid.uuid4())
            movie = m['movie']
            character_name = q['character_name']
            imdb_link = q['imdb_link']
            quote = q['imdb_link']
            response = dynamodb.put_item(Item=q)
            return response
        except Exception as e:
            logger.error(str(e))
            return {'Error': str(e)}


@logger.inject_lambda_context
def lambda_handler(event, context):
    """Sample pure Lambda function

    Parameters
    ----------
    event: dict, required
        API Gateway Lambda Proxy Input Format

        Event doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html#api-gateway-simple-proxy-for-lambda-input-format

    context: object, required
        Lambda Context runtime methods and attributes

        Context doc: https://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html

    Returns
    ------
    API Gateway Lambda Proxy Output Format: dict

        Return doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html
    """
    logger.info({
        'event': event
    })

    if event['body'] == None:
        return {
            "headers": {
                "content-type": "application/json"
            },
            "statusCode": 500,
            "body": 'An error was encountered'
        }
    
    body = json.loads(event['body'])
    
    response = post_quotes(body)
    
    if 'Error' in response:
        return {
            "headers": {
                "content-type": "application/json"
            },
            "statusCode": 500,
            "body": 'An error was encountered'
        }
    else:
        return {
            "headers": {
                "content-type": "application/json"
            },
            "statusCode": 200,
            "body": "success"
        }
