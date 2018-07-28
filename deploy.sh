zip -r ../package.zip . 
aws s3 cp ../package.zip s3://deploy-prd
