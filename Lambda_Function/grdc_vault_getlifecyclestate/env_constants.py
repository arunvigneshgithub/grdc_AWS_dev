
"""
Created By      : Arun Vignesh
Description     : Environment related constants for Migration to different environments.
"""
server = "https://sb-sanofi-rim-idmp-sandbox-2.veevavault.com"
#server = "https://sanofi-rim-production.veevavault.com"
auth_url = "https://sb-sanofi-rim-idmp-sandbox-2.veevavault.com/api/v23.1/auth"
secret_arn = "arn:aws:secretsmanager:eu-west-1:622440229310:secret:CREDENTIALS-nbuEYd"
scrt_fldr = 'VEEVA_VAULT_MIG4'
pay_load = {'username': 'abc', 'password': 'def'}
version = "/api/v18.3"
login_headers = {'Content-Type': 'application/x-www-form-urlencoded', 'Accept': 'application/json'}
proxies = {'http':'http://emea-aws-webproxy.service.cloud.local:3128','https':'http://emea-aws-webproxy.service.cloud.local:3128'}
region_name = "eu-west-1"
metadata_payload = {}
obj_payload = {}
##veeva_url = "https://sbsanofi-rim-migration-4.veevavault.com/api/v18.3/"

