import json
import boto3
import logging
from boto3.dynamodb.conditions import Key

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('StudentRecords')

def lambda_handler(event, context):
    try:
        if event['httpMethod'] == 'POST':
            # Create a new student record
            student = json.loads(event['body'])
            if not all(key in student for key in ['student_id', 'name', 'course']):
                return {
                    'statusCode': 400,
                    'body': json.dumps('Missing required fields')
                }
            table.put_item(Item=student)
            return {
                'statusCode': 200,
                'body': json.dumps('Student record added successfully')
            }

        elif event['httpMethod'] == 'GET':
            # Fetch student record by student_id
            if 'queryStringParameters' not in event or 'student_id' not in event['queryStringParameters']:
                return {
                    'statusCode': 400,
                    'body': json.dumps('Missing student_id in query parameters')
                }
            student_id = event['queryStringParameters']['student_id']
            response = table.get_item(Key={'student_id': student_id})
            return {
                'statusCode': 200,
                'body': json.dumps(response.get('Item', {}))
            }

        elif event['httpMethod'] == 'PUT':
            # Update student details
            student = json.loads(event['body'])
            student_id = student.get('student_id')
            name = student.get('name')
            course = student.get('course')
            if not student_id:
                return {
                    'statusCode': 400,
                    'body': json.dumps('Missing student_id')
                }
            update_expression = 'SET '
            expression_attribute_values = {}
            expression_attribute_names = {}
            update_parts = []
            if name:
                update_parts.append('#n = :n')
                expression_attribute_values[':n'] = name
                expression_attribute_names['#n'] = 'name'
            if course:
                update_parts.append('#c = :c')
                expression_attribute_values[':c'] = course
                expression_attribute_names['#c'] = 'course'
            if not update_parts:
                return {
                    'statusCode': 400,
                    'body': json.dumps('No fields to update')
                }
            update_expression += ', '.join(update_parts)
            response = table.update_item(
                Key={'student_id': student_id},
                UpdateExpression=update_expression,
                ExpressionAttributeValues=expression_attribute_values,
                ExpressionAttributeNames=expression_attribute_names,
                ReturnValues='UPDATED_NEW'
            )
            return {
                'statusCode': 200,
                'body': json.dumps('Student record updated successfully')
            }

        elif event['httpMethod'] == 'DELETE':
            # Delete student record
            if 'queryStringParameters' not in event or 'student_id' not in event['queryStringParameters']:
                return {
                    'statusCode': 400,
                    'body': json.dumps('Missing student_id in query parameters')
                }
            student_id = event['queryStringParameters']['student_id']
            response = table.delete_item(
                Key={'student_id': student_id},
                ReturnValues='ALL_OLD'
            )
            if 'Attributes' not in response:
                return {
                    'statusCode': 404,
                    'body': json.dumps('Student record not found')
                }
            return {
                'statusCode': 200,
                'body': json.dumps('Student record deleted successfully')
            }

        else:
            return {
                'statusCode': 404,
                'body': json.dumps('Not Found')
            }

    except json.JSONDecodeError as e:
        logger.error(f"JSON Decode Error: {str(e)}")
        return {
            'statusCode': 400,
            'body': json.dumps('Invalid JSON in request body')
        }
    except boto3.exceptions.Boto3Error as e:
        logger.error(f"Boto3 Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps('Error interacting with DynamoDB')
        }
    except Exception as e:
        logger.error(f"Unexpected Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps('Internal Server Error')
        }
