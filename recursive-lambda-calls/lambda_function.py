import json
import boto3

COUNTER = 'COUNTER'
# Initialize any time consuming operations like DB connections here
# as AWS initialises a container once and reuses the same container if called within a particular time frame.
# It will apply to our use case as we call the same Lambda again after the current one.
s3 = boto3.resource('s3')
source_bucket = s3.Bucket('source-bucket')
dest_bucket = s3.Bucket('dest-bucket')


def lambda_handler(event, context):
    processor = Processor(event)
    return processor.process(context, _get_items_to_process(), _process_item)


def _get_items_to_process():
    return source_bucket.objects.all()


def _process_item(item):
    # processing code goes here
    # below code is for illustration only. There are better ways to copy S3 objects from one bucket to other.
    dest_object = s3.Object(dest_bucket.name, item.key)
    dest_object.copy_from(CopySource={
        'Bucket': source_bucket.name,
        'Key': item.key
    })
    item.delete()


class Processor:
    def __init__(self, event):
        self.event = event
        self.counter = 0
        if event.get(COUNTER) is not None:
            self.counter = event[COUNTER]

    def process(self, context, items, function):
        for item in items:
            if context.get_remaining_time_in_millis() < 10000:
                print("Processed " + str(self.counter) + " before recursion")
                self._make_recursive_call(context)
                return  # necessary to end the function after recursion to avoid multiple recursive calls
            else:
                function(item)
                self.counter = self.counter + 1
                if self.counter % 1000 == 0:
                    print(" Processed " + str(counter))
        print("All " + str(self.counter) + " files processed")
        return

    def _make_recursive_call(self, context):
        # the event object can be used to pass state for the subsequent lambda invocations
        self.event[COUNTER] = self.counter
        lambda_client = boto3.client('lambda')
        lambda_client.invoke(FunctionName=context.function_name,
                             InvocationType='Event',
                             Payload=json.dumps(self.event))
        return
