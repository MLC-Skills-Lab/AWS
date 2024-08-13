import json
import boto3
import mimetypes
from PIL import Image
from io import BytesIO

def lambda_handler(event, context):
    s3_client = boto3.client('s3')
    sns_client = boto3.client('sns')
    # Get the bucket name and object key from the event
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    object_key = event['Records'][0]['s3']['object']['key']
    destination_bucket = 'mythumket'
    thumbnail_key = object_key
    
    key,types=mimetypes.guess_type(object_key)
    allowed_type=['image/jpeg', 'image/png']
    if key not in allowed_type:
        s3_client.delete_object(Bucket=bucket_name, Key=object_key)
    else:
        try:
            s3_response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
            print("Executed")
            image_data = s3_response['Body'].read()
                    
            # Create a thumbnail
            im = Image.open(BytesIO(image_data))
            image = im.convert('RGB')
            image.thumbnail((128, 128))  # Resize the image to a 128x128 thumbnail
            buffer = BytesIO()
            image.save(buffer, 'JPEG')
            buffer.seek(0)
            
            s3_client.put_object(Bucket=destination_bucket, Key=thumbnail_key, Body=buffer, ContentType='image/jpeg')
            print(f'Thumbnail created : {destination_bucket} / {thumbnail_key}')
        except Exception as e:
            print(e)
    thumbnailurl=f'https://{destination_bucket}.s3.amazonaws.com/{thumbnail_key}'
    message = {
            "File_name" : object_key,
            "URL" : thumbnailurl
        }
        
    sns_response = sns_client.publish(
        TopicArn='arn:aws:sns:ap-south-1:381492036866:notification',
        Message=json.dumps(message),
        Subject='File Uploaded successfully',
        )

    return {
        'statusCode': 200,
        'body': json.dumps("Execution sucessfully")
    }
    
    