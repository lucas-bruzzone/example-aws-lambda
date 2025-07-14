import json
import os

def lambda_handler(event, context):
    """
    Lambda handler function
    """
    
    environment = os.environ.get('ENVIRONMENT', 'unknown')
    
    response = {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
            'Access-Control-Allow-Methods': 'POST,OPTIONS'
        },
        'body': json.dumps({
            'message': 'Hello from Lambda!',
            'environment': environment,
            'event': event
        })
    }
    
    return response