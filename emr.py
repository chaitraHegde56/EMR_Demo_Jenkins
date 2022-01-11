import boto3
import sys
import botocore
import json
import time
import logging
import argparse

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


class EMRLoader(object):
    def __init__(self, indicies, executor_memory, driver_memory, aws_access_key, aws_secret_access_key,
                 riskdb_access_key, riskdb_secret_key, riskdb_region, region_name,
                 cluster_name, instance_count, master_instance_type, slave_instance_type,
                 key_name, subnet_id, log_uri, software_version, script_bucket_name):
        self.indicies = indicies
        self.executor_memory = executor_memory
        self.driver_memory = driver_memory
        self.aws_access_key = aws_access_key
        self.aws_secret_access_key = aws_secret_access_key
        self.riskdb_access_key = riskdb_access_key
        self.riskdb_secret_key = riskdb_secret_key
        self.riskdb_region = riskdb_region
        self.region_name = region_name
        self.cluster_name = cluster_name
        self.instance_count = instance_count
        self.master_instance_type = master_instance_type
        self.slave_instance_type = slave_instance_type
        self.key_name = key_name
        self.subnet_id = subnet_id
        self.log_uri = log_uri
        self.software_version = software_version
        self.script_bucket_name = script_bucket_name

    def boto_client(self, service):
        client = boto3.client(service,
                              aws_access_key_id=self.aws_access_key,
                              aws_secret_access_key=self.aws_secret_access_key,
                              region_name=self.region_name)
        return client

    def nonguv_boto_client(self, service):
        client = boto3.client(service,
                              aws_access_key_id=self.riskdb_access_key,
                              aws_secret_access_key=self.riskdb_secret_key,
                              region_name=self.riskdb_region)
        return client

    def associate_eip(self, instance_id, allocation_id):
        response = self.boto_client('ec2').associate_address(
            AllocationId=allocation_id,
            InstanceId=instance_id)
        return response

    def disassociate_eip(self, association_id):
        response = self.boto_client('ec2').disassociate_address(
            AssociationId=association_id)
        return response

    def load_cluster(self):
        response = self.boto_client("emr").run_job_flow(
            Name=self.cluster_name,
            LogUri=self.log_uri,
            ReleaseLabel=self.software_version,
            Instances={
                'MasterInstanceType': self.master_instance_type,
                'SlaveInstanceType': self.slave_instance_type,
                'InstanceCount': self.instance_count,
                'KeepJobFlowAliveWhenNoSteps': True,
                'TerminationProtected': False,
                'Ec2KeyName': self.key_name,
                'Ec2SubnetId': self.subnet_id
            },
            Applications=[
                {
                    'Name': 'Spark'
                }
            ],
            BootstrapActions=[
                {
                    'Name': 'Install Anaconda',
                    'ScriptBootstrapAction': {
                        'Path': 's3://{script_bucket_name}/bootstrap.sh'.format(
                            script_bucket_name=self.script_bucket_name),
                    }
                },
            ],
            VisibleToAllUsers=True,
            EbsRootVolumeSize=30,
            JobFlowRole='EMR_EC2_DefaultRole',
            ServiceRole='EMR_DefaultRole'
        )
        logger.info(response)
        return response

    def add_step(self, job_flow_id, master_dns):
        response = self.boto_client("emr").add_job_flow_steps(
            JobFlowId=job_flow_id,
            Steps=[
                {
                    'Name': 'Setup Python3 for Spark',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['sudo', 'sed', '-i', '-e', '$a\export PYSPARK_PYTHON=/usr/bin/python3',
                                 '/etc/spark/conf/spark-env.sh']
                    }
                },
                # {
                #     'Name': 'Setup - Copy src-dependencies.zip',
                #     'ActionOnFailure': 'CANCEL_AND_WAIT',
                #     'HadoopJarStep': {
                #         'Jar': 'command-runner.jar',
                #         'Args': ['aws', 's3', 'cp',
                #                  's3://{script_bucket_name}/src-dependencies.zip'.format(
                #                      script_bucket_name=self.script_bucket_name),
                #                  '/home/hadoop/']
                #     }
                # },
                {
                    'Name': 'Setup - Create riskdb dir',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['mkdir', '/home/hadoop/riskdb']
                    }
                },
                # {
                #     'Name': 'Setup - unzip src-dependencies.zip',
                #     'ActionOnFailure': 'CANCEL_AND_WAIT',
                #     'HadoopJarStep': {
                #         'Jar': 'command-runner.jar',
                #         'Args': ['unzip', '/home/hadoop/src-dependencies.zip', '-d',
                #                  '/home/hadoop/riskdb/riskfusion-pyspark']
                #     }
                # },
                # {
                #     'Name': 'Setup - Copy dependencies.zip',
                #     'ActionOnFailure': 'CANCEL_AND_WAIT',
                #     'HadoopJarStep': {
                #         'Jar': 'command-runner.jar',
                #         'Args': ['aws', 's3', 'cp',
                #                  's3://{script_bucket_name}/dependencies.zip'.format(
                #                      script_bucket_name=self.script_bucket_name),
                #                  '/home/hadoop/']
                #     }
                # },


                # Test emr

                {
                    'Name': 'Setup - Create dist dir',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['mkdir', '/home/hadoop/dist']
                    }
                },


                {
                    'Name': 'Setup - Copy src-dependencies.zip',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['aws', 's3', 'cp',
                                 's3://{script_bucket_name}/src.zip'.format(
                                     script_bucket_name=self.script_bucket_name),
                                 '/home/hadoop/dist/']
                    }
                },
                {
                    'Name': 'Setup - Copy main file',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['aws', 's3', 'cp',
                                 's3://{script_bucket_name}/main.py'.format(
                                     script_bucket_name=self.script_bucket_name),
                                 '/home/hadoop/dist/']
                    }
                },
                {
                    'Name': 'Setup - Copy config',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['aws', 's3', 'cp',
                                 's3://{script_bucket_name}/config.yaml'.format(
                                     script_bucket_name=self.script_bucket_name),
                                 '/home/hadoop/dist/config.yaml']
                    }
                },
                {
                    'Name': 'Run EMR demo',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['sudo','spark-submit', '--deploy-mode', 'cluster', '--master', 'yarn-cluster',
                                 '--files', '/home/hadoop/dist/config.yaml#config.yaml',
                                 '--driver-memory', self.driver_memory,
                                 '--executor-memory', self.executor_memory,
                                 '--py-files', '/home/hadoop/dist/src.zip',
                                 '/home/hadoop/dist/main.py']
                    }
                },



                # Test emr end


                # {
                #     'Name': 'Run Threat Spark Job',
                #     'ActionOnFailure': 'CANCEL_AND_WAIT',
                #     'HadoopJarStep': {
                #         'Jar': 'command-runner.jar',
                #         'Args': ['spark-submit', '--deploy-mode', 'cluster', '--master', 'yarn-cluster',
                #                  '--files',
                #                  '/home/hadoop/riskdb/riskfusion-pyspark/config.yaml#config.yaml,/home/hadoop/riskdb/riskfusion-pyspark/category_mapping.json#category_mapping.json,/home/hadoop/riskdb/riskfusion-pyspark/ransomware_family_mapping.json#ransomware_family_mapping.json',
                #                  '--packages', 'com.datastax.spark:spark-cassandra-connector_2.11:2.3.0',
                #                  '--driver-class-path', '/home/hadoop/jars/mysql-connector-java-8.0.12.jar',
                #                  '--driver-memory', self.driver_memory,
                #                  '--executor-memory', self.executor_memory,
                #                  '--py-files', '/home/hadoop/dependencies.zip',
                #                  '--jars',
                #                  '/home/hadoop/jars/aws-java-sdk-1.7.4.jar,/home/hadoop/jars/hadoop-aws-2.7.3.jar,/home/hadoop/jars/elasticsearch-spark-20_2.11-6.2.3.jar,/home/hadoop/jars/mysql-connector-java-8.0.12.jar',
                #                  '/home/hadoop/riskdb/riskfusion-pyspark/src/riskfusion_indices.py',
                #                  '--index-type', self.indicies]
                #     }
                # },
            ]
        )
        logger.info(response)
        return response

    def create_bucket_on_s3(self, bucket_name):
        print("\n\n\n\n")
        print(self.region_name)
        print("\n\n\n\n")
        s3 = self.boto_client("s3")
        try:
            logger.info("Bucket already exists.")
            s3.head_bucket(Bucket=bucket_name)
        except botocore.exceptions.ClientError as e:
            logger.info("Bucket does not exist: {error}. I will create it!".format(error=e))
            s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={'LocationConstraint': self.region_name})

    def upload_to_s3(self, file_name, bucket_name, key_name):
        s3 = self.boto_client("s3")
        logger.info(
            "Upload file '{file_name}' to bucket '{bucket_name}'".format(file_name=file_name, bucket_name=bucket_name))
        s3.upload_file(file_name, bucket_name, key_name)


def main():
    parser = argparse.ArgumentParser(description='Threat EMR Program:')
    parser.add_argument('--indicies', required=True, help='Indicies that must be processed')

    args = parser.parse_args()

    with open("config.json", "r") as file:
        config = json.load(file)

    config_emr = config.get("config")

    emr_loader = EMRLoader(
        indicies=args.indicies,
        executor_memory=config_emr.get("executor_memory"),
        driver_memory=config_emr.get("driver_memory"),
        aws_access_key=config_emr.get("aws_access_key"),
        aws_secret_access_key=config_emr.get("aws_secret_access_key"),
        riskdb_access_key=config_emr.get("riskdb_access_key"),
        riskdb_secret_key=config_emr.get("riskdb_secret_key"),
        riskdb_region=config_emr.get("riskdb_region"),
        region_name=config_emr.get("aws_region"),
        cluster_name=config_emr.get("emr_cluster_name"),
        instance_count=config_emr.get("instances"),
        master_instance_type=config_emr.get("master_instance"),
        slave_instance_type=config_emr.get("slave_instance"),
        key_name=config_emr.get("key_pair"),
        subnet_id=config_emr.get("subnet_id"),
        log_uri=config_emr.get("s3_logs"),
        software_version=config_emr.get("emr_version"),
        script_bucket_name=config_emr.get("script_bucket_name")

    )

    logger.info("Check if bucket exists otherwise create it and upload files to S3.")

    emr_loader.create_bucket_on_s3(bucket_name=config_emr.get("s3_logs_bucket"))

    emr_loader.create_bucket_on_s3(bucket_name=config_emr.get("script_bucket_name"))
    emr_loader.upload_to_s3("scripts/bootstrap.sh", bucket_name=config_emr.get("script_bucket_name"),
                            key_name="bootstrap.sh")
    emr_loader.upload_to_s3("scripts/auto_update_permissions.sh", bucket_name=config_emr.get("script_bucket_name"),
                            key_name="auto_update_permissions.sh")
    emr_loader.upload_to_s3("scripts/aws_env.sh", bucket_name=config_emr.get("script_bucket_name"),
                            key_name="aws_env.sh")
    emr_loader.upload_to_s3("scripts/spark.sh", bucket_name=config_emr.get("script_bucket_name"),
                            key_name="spark.sh")
    emr_loader.upload_to_s3("dist/src.zip", bucket_name=config_emr.get("script_bucket_name"),
                                                    key_name="src.zip")
    emr_loader.upload_to_s3("dist/main.py", bucket_name=config_emr.get("script_bucket_name"),
                                                    key_name="main.py")
    emr_loader.upload_to_s3("dist/config.yaml", bucket_name=config_emr.get("script_bucket_name"),
                                                    key_name="config.yaml")
    # emr_loader.upload_to_s3("files/dependencies.zip", bucket_name=config_emr.get("script_bucket_name"),
    #                         key_name="dependencies.zip")
    # emr_loader.upload_to_s3("files/src-dependencies.zip", bucket_name=config_emr.get("script_bucket_name"),
    #                         key_name="src-dependencies.zip")
    # emr_loader.upload_to_s3("files/mysql-connector-java-8.0.12.jar", bucket_name=config_emr.get("script_bucket_name"),
    #                         key_name="mysql-connector-java-8.0.12.jar")
    # emr_loader.upload_to_s3("files/aws-java-sdk-1.7.4.jar", bucket_name=config_emr.get("script_bucket_name"),
    #                         key_name="aws-java-sdk-1.7.4.jar")
    # emr_loader.upload_to_s3("files/hadoop-aws-2.7.3.jar", bucket_name=config_emr.get("script_bucket_name"),
    #                         key_name="hadoop-aws-2.7.3.jar")
    # emr_loader.upload_to_s3("files/elasticsearch-spark-20_2.11-6.2.3.jar",
    #                         bucket_name=config_emr.get("script_bucket_name"),
    #                         key_name="elasticsearch-spark-20_2.11-6.2.3.jar")

    logger.info("Launch cluster")
    emr_response = emr_loader.load_cluster()
    emr_client = emr_loader.boto_client("emr")

    while True:
        job_response = emr_client.describe_cluster(
            ClusterId=emr_response.get("JobFlowId")
        )
        time.sleep(10)
        if job_response.get("Cluster").get("MasterPublicDnsName") is not None:
            master_dns = job_response.get("Cluster").get("MasterPublicDnsName")

        step = True

        job_state = job_response.get("Cluster").get("Status").get("State")
        job_state_reason = job_response.get("Cluster").get("Status").get("StateChangeReason").get("Message")

        if job_state in ["WAITING"]:
            step = True
            logger.info("Cluster is at '{job_state}' state "
                        "and {job_state_reason}".format(job_state=job_state, job_state_reason=job_state_reason))
            break
        elif job_state in ["TERMINATED", "TERMINATED_WITH_ERRORS"]:
            step = False
            logger.info("Script stops with state: {job_state} "
                        "and reason: {job_state_reason}".format(job_state=job_state, job_state_reason=job_state_reason))
            break
        else:
            cluster = job_response.get('Cluster')
            cluster_name = cluster.get('Name')
            cluster_status = cluster.get('Status').get('State')

            logger.info("Cluster ({cluster_name}) status: {cluster_status}".format(cluster_name=cluster_name,
                                                                                   cluster_status=cluster_status))

    #
    if step:
        logger.info("Run steps.")
        add_step_response = emr_loader.add_step(emr_response.get("JobFlowId"), master_dns)

        while True:
            list_steps_response = emr_client.list_steps(ClusterId=emr_response.get("JobFlowId"),
                                                        StepStates=["COMPLETED"])
            time.sleep(20)
            if len(list_steps_response.get("Steps")) == len(
                    add_step_response.get("StepIds")):  # make sure that all steps are completed
                break
            else:
                logger.info(emr_client.list_steps(ClusterId=emr_response.get("JobFlowId")))
    else:
        logger.info("Cannot run steps.")

    # time.sleep(2400)
    #
    # Identify the Cluster IPs and Set the permissions
    #
    ec2_client = emr_loader.nonguv_boto_client("ec2")

    JobFlowId = emr_response.get("JobFlowId")

    job_response = emr_client.list_instances(ClusterId=JobFlowId)

    for instance_row in job_response['Instances']:

        publicIP = instance_row['PublicIpAddress']

        logger.info("Removing the public-ip {ip}".format(ip=publicIP))

        CidrIp = publicIP + '/32'

        res = ec2_client.revoke_security_group_ingress(CidrIp=CidrIp,
                                                       GroupId='sg-4e94aa35',
                                                       IpProtocol='tcp',
                                                       FromPort=3306,
                                                       ToPort=3306)

        if res['ResponseMetadata']['HTTPStatusCode'] == 200:

            logger.info("Revoking the public-ip is successful")
        else:

            logger.error("Problem revoking the public-ip {ip}".format(ip=publicIP))

    # Terminate EMR
    terminate_response = emr_client.terminate_job_flows(JobFlowIds=[JobFlowId])

    if terminate_response['ResponseMetadata']['HTTPStatusCode'] == 200:

        logger.info("Cluster successfully terminated")
    else:

        logger.error("Problem terminating cluster")


if __name__ == "__main__":
    main()