from django.urls import path, include

from rest_framework import routers
from .views import S3BucketViewset


urlpatterns = [
    path("s3", S3BucketViewset.as_view(
        {"get": "list"}), name="s3_bucket_list"),
    path("s3/<str:bucket_name>", S3BucketViewset.as_view(
        {"get": "get_object"}), name="s3_bucket_detail"),  
    path("s3/process_file/", S3BucketViewset.as_view(
        {"post": "process_file"}), name="process_file"), 
]