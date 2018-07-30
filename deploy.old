zip -r ../package.zip . 
aws s3 cp ../package.zip s3://deploy-prd
aws lambda update-function-code --function-name 'cg-load-stg' --s3-bucket 'deploy-prd' --s3-key 'package.zip'
echo "Lambda atualizado."
