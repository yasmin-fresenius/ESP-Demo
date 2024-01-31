import boto3
from rest_framework import status
from rest_framework.response import Response
from rest_framework.viewsets import ModelViewSet
from dsc_demo.env import config
import pandas as pd
import json
from django.http import HttpResponse

class S3BucketViewset(ModelViewSet):
   client = boto3.client('s3', 
                         aws_access_key_id=config('AWS_ACCESS_KEY_ID'),
                         aws_secret_access_key=config('AWS_SECRET_ACCESS_KEY'),
                         region_name=config('REGION_NAME')
                         )
   queryset = client.list_buckets()
   lookup_field = 'bucket_name'
                                                                                                        
   def list(self, request, *args, **kwargs):
        """
        List all buckets
        """ 
        list_of_buckets = self.client.list_buckets().get('Buckets', [])
        return Response({"buctket_list": list_of_buckets}, status=200)
   
   def get_object(self, request, *args, **kwargs):
       """List all the files."""
       list_of_files = self.client.list_objects(Bucket=self.kwargs[self.lookup_field])
       contents = list_of_files.get('Contents')
       return Response({"file_list": contents}, status=200)
   
   def process_file(self, request, *args, **kwargs):
       """
       Assuming the file and config file received from lambda trigger.
       Assuming file as csv.
       """
       file, config_file = None, None 
       file = self.request.FILES.get('file')
       config_file = self.request.FILES.get('config_file')
       
       if file is None or config_file is None:
           return Response({"message": "File or config file not found"}, status=404)
       
       df = pd.read_csv(file)
       config_df = pd.read_csv(config_file)
       
       config_json = config_df.to_json(orient='records')
       for col in df.columns:
            if col in config_df.columns and (config_df[col] == 'Mask').any():
                df[col] = df[col].apply(lambda x: '********')
                   
       #response = HttpResponse(content_type='text/csv')
       #response['Content-Disposition'] = 'attachment; filename="processed_file.csv"'
       #df.to_csv(path_or_buf=response, index=False)
       df.to_csv('processed_file.csv', index=False)
    
       return Response("File processed successfully", status=200)
       #return response
       
       

       

       
        
   
    