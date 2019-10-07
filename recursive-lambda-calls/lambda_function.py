import json

import boto3

COUNTER = 'COUNTER'
s3 = boto3.resource('s3')
source_bucket = s3.Bucket('test-aws-lambda-s3')
dest_bucket = s3.Bucket('bsa-artifacts')


def lambda_handler(event, context):
    counter = 0
    if event.get(COUNTER) is not None:
        counter = event[COUNTER]

    items = source_bucket.objects.all()

    for item in items:
        if context.get_remaining_time_in_millis() < 10000:
            print("Processed " + str(counter) + " before recursion")
            _make_recursive_call(event, context, counter)
            return  # necessary to end the function after recursion to avoid multiple recursive calls
        else:
            _process_item(item)
            counter = counter + 1
            if counter % 1000 == 0:
                print(" Processed " + str(counter))
    print("All " + str(counter) + " files processed")
    return


def _make_recursive_call(event, context, counter):
    event[COUNTER] = counter
    lambda_client = boto3.client('lambda')
    lambda_client.invoke(FunctionName=context.function_name, InvocationType='Event', Payload=json.dumps(event))
    return


def _process_item(item):
    # processing code goes here
    # print(item.key)
    dest_object = s3.Object(dest_bucket.name, item.key)
    dest_object.copy_from(CopySource={
        'Bucket': source_bucket.name,
        'Key': item.key
    })
    item.delete()
