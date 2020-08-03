import json
import os

from aws_lambda_powertools import Logger
import boto3


aws_region = os.getenv('AWS_REGION')

table_name = os.getenv('QUOTES_TABLE')

dynamodb = boto3.client('dynamodb', region_name=aws_region)

logger = Logger()


def scan_quotes(movie=None, character_name=None):
    """Look for quotes based on movie name or character

    Parameters
    ----------
    movie_name: string (default: None)
        Name of movie to scan
    
    character: string (default: None)
        Name of character to scan
    
    Returns
    -------
    Quote: dict
        Returns movie_name, character, imdb_link, quote
    """
    if movie and character_name:
        logger.info({
            'movie': movie,
            'character_name': character_name
        })
        try:
            response = dynamodb.scan(
                TableName=table_name,
                FilterExpression='movie = :mn and character_name = :c',
                ExpressionAttributeValues= {
                    ':mn': {'S': movie},
                    ':c': {'S': character_name}
                }
            )
            logger.info({
                'results': len(response['Items']),
                'items': response['Items']
            })
            return json.dumps(response['Items'])
        except Exception as e:
            logger.error(str(e))
            return {'Error': str(e)}
        
    if movie:
        logger.info({
            'movie': movie
        })
        try:
            response = dynamodb.scan(
                TableName=table_name,
                FilterExpression='movie = :mn',
                ExpressionAttributeValues= {
                    ':mn': {'S': movie}
                }
            )
            logger.info({
                'results': len(response['Items']),
                'items': response['Items']
            })
            return json.dumps(response['Items'])
        except Exception as e:
            logger.error(str(e))
            return {'Error': str(e)}
    
    if character_name:
        logger.info({
            'character_name': character_name
        })
        try:
            response = dynamodb.scan(
                TableName=table_name,
                FilterExpression='character_name = :c',
                ExpressionAttributeValues= {
                    ':c': {'S': character_name}
                }
            )
            logger.info({
                'results': len(response['Items']),
                'items': response['Items']
            })
            return json.dumps(response['Items'])
        except Exception as e:
            logger.error(str(e))
            return {'Error': str(e)}
    
    try:
        response = dynamodb.scan(
                    TableName=table_name
        )
        logger.info({
            'results': len(response['Items']),
            'items': response['Items']
        })
        return json.dumps(response['Items'])
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

    logger.info({
        'queryStringParameters': event['queryStringParameters']
    })

    if event['queryStringParameters'] == None:
        response = scan_quotes()

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
                "body": response
            }

    if 'character_name' in event['queryStringParameters'] and 'movie' in event['queryStringParameters']:
        character_name = event['queryStringParameters']['character_name']
        movie = event['queryStringParameters']['movie']
        response = scan_quotes(movie=movie, character_name=character_name)
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
                "body": response
            }
    
    if 'character_name' in event['queryStringParameters'] and 'movie' not in event['queryStringParameters']:
        character_name = event['queryStringParameters']['character_name']
        response = scan_quotes(character_name=character_name)
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
                "body": response
            }
    
    if 'movie' in event['queryStringParameters'] and 'character_name' not in event['queryStringParameters']:
        movie = event['queryStringParameters']['movie']
        response = scan_quotes(movie=movie)
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
                "body": response
            }
    
    