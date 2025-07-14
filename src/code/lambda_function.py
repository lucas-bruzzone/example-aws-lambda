import json
import os

def lambda_handler(event, context):
    """
    Lambda handler function
    """
    
    environment = os.environ.get('ENVIRONMENT', 'unknown')
    
    response = {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'Hello from Lambda!',
            'environment': environment,
            'event': event
        })
    }
    
    return response