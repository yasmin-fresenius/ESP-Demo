import re
from tkinter import E
import boto3
from rest_framework import status
from rest_framework.response import Response
from rest_framework.viewsets import ModelViewSet
from dsc_demo.env import config
import pandas as pd
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad
import numpy as np
import datetime

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
       try:
            # Get the file from S3 bucket
            list_of_files = self.client.list_objects(Bucket=self.kwargs[self.lookup_field])
            contents = list_of_files.get('Contents')

            # Retrieve the config file from request.FILES
            config_file = request.FILES.get('config_file')

            if config_file is None:
                return Response({"message": "Config file not found"}, status=404)

            # Read config file into a DataFrame
            config_df = pd.read_csv(config_file, delimiter=',')

            # Find the corresponding file based on DatasetName in config file
            file = next((obj['Key'] for obj in contents if obj['Key'] == config_df['Dataset Name'].iloc[0]), None)

            if file is None:
                return Response({"message": "File specified in config not found"}, status=404)

            # Read the file from S3 into DataFrame
            df = pd.read_csv(self.client.get_object(Bucket=self.kwargs[self.lookup_field], Key=file)['Body'], delimiter=',')
            # print(df.columns)
            # print(df.head())
            # for col in df.columns:
            #     print(col,': ', df[col].dtype)

            # Perform AES encryption/anonymization
            with open("key.pem", "rb") as f:
                key = f.read()
            cipher = AES.new(key, AES.MODE_ECB)

            # columns_to_mask = config_df.loc[config_df['Deidentify (y/n)'] == 'y', 'Field'].tolist()

            deidentify_type = config_df['Deidentify Method (optional)'].iloc[0]
            if deidentify_type is np.nan:
               deidentify_type = 'anonymyzation'

            data_type = {x['Field']: x['Data Type'] for _, x in config_df.iterrows() if x['Deidentify (y/n)'] == 'y' }

            if deidentify_type == 'anonymyzation': # Masking type
                for col in df.columns:
                    encrypted_data_list = []
                    for value in df[col]:
                        ct_bytes = pad(str(value).encode(), AES.block_size)
                        encrypted_data = cipher.encrypt(ct_bytes)
                        hex_encrypted_data = encrypted_data.hex()
                        if data_type.get(col) == 'number':
                            truncated_hex_encrypted_data = hex_encrypted_data[:6]
                            # Convert the truncated hex string to an integer
                            if df[col].dtype in ['int64', 'int32']:
                                numeric_value = int(truncated_hex_encrypted_data, 16)
                            # Convert the truncated hex string to a float
                            elif df[col].dtype in ['float64', 'float32']:
                                numeric_value = float(int(truncated_hex_encrypted_data, 16))
                                numeric_value = "{:.2f}".format(numeric_value)
                            encrypted_data_list.append(numeric_value)
                        elif data_type.get(col) == 'text':
                            # Remove all non-alphabetic characters
                            alpha_encrypted_data = ''.join(filter(str.isalpha, hex_encrypted_data))
                            encrypted_data_list.append(alpha_encrypted_data)
                        elif data_type.get(col) == 'date':
                            value = datetime.datetime.strptime(value, '%d.%m.%Y').date()
                            key = 8479
                            enc_value = value + datetime.timedelta(days=key)
                            encrypted_data_list.append(enc_value)
                        else:
                            # No encryption for other data types
                            encrypted_data_list.append(value)

                    df[col] = encrypted_data_list

            # Save the modified DataFrame to a new CSV file
            df.to_csv("Encrypted_employee.csv", index=False)

            return Response({"message": "File processed successfully"}, status=200)

       except Exception as e:
            return Response({"message": str(e)}, status=500)
