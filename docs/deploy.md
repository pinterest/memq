# Deploying MemQ
Below is the guide on how to deployment MemQ. We recommend you read the entire guide before starting to avoid any blockers as there are external systems MemQ depends on.

## Pre-requisites

The following systems need to be deployed and running before we can deploy a MemQ cluster. We have also added links to how to deploy these clusters but are not going to go into how to do that in this guide.

- [Kafka cluster](https://kafka.apache.org/quickstart)
- [Amazon S3 Bucket](https://aws.amazon.com/getting-started/)
- [Zookeeper cluster](https://zookeeper.apache.org/doc/r3.3.3/zookeeperAdmin.html) (*if using clustered mode*)

**Note: When creating S3 bucket please make sure to provision IAM Instance Profile credentials for the MemQ Broker hosts and MemQ Consumer**

### Non-clustered Deployment (simple deployment)
See `deploy/configs/nonclustered.yaml`

### Clustered Deployment (complex deployment)
See `deploy/configus/clustered.yaml`

#### S3 IAM Configuration Pointers

Below is a sample terraform template for MemQ IAM policy, the same policy can be used for Broker and Consumer. You can also tweak Consumer policy to make it limit it to `Get*` and `List*`

Please don't forget to attach your policy to your IAM role.

```
resource "aws_iam_policy" "MyMemQPolicy" {
  name        = "MyMemQPolicy"
  path        = "/"
  description = ""

  policy = <<POLICY
{
  "Statement": [
    {
      "Action": [
        "s3:Abort*",
        "s3:DeleteO*",
        "s3:Get*",
        "s3:List*",
        "s3:PutO*",
        "s3:RestoreObject"
      ],
      "Effect": "Allow",
      "Resource": [
        "arn:aws:s3:::<yours3bucket>",
        "arn:aws:s3:::<yours3bucket>/*"
      ],
      "Sid": "MemQAccess"
    }
  ],
  "Version": "2012-10-17"
}

POLICY
}
```
