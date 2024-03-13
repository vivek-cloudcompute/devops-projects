import boto3
from datetime import datetime, timedelta, timezone

def lambda_handler(event, context):
    ec2 = boto3.client('ec2')
    sts = boto3.client('sts')
    
    # Get the AWS account ID
    account_id = sts.get_caller_identity()['Account']
    
    # Get current date
    current_date = datetime.utcnow().replace(tzinfo=timezone.utc)
    
    # Get all snapshots
    snapshots = ec2.describe_snapshots(OwnerIds=[account_id])['Snapshots']
    
    # Filter snapshots not associated with a volume or EC2 instance
    orphan_snapshots = [snapshot for snapshot in snapshots if 'VolumeId' not in snapshot]
    
    # Filter snapshots associated with EC2 instances but not running
    running_instances = []
    reservations = ec2.describe_instances()['Reservations']
    for reservation in reservations:
        for instance in reservation.get('Instances', []):
            instance_id = instance.get('InstanceId')
            if instance_id:
                running_instances.append(instance_id)
    
    associated_snapshots = [snapshot for snapshot in snapshots if 'VolumeId' in snapshot and snapshot['VolumeId'] not in running_instances]
    
    # Filter snapshots older than 6 months and never used
    six_months_ago = current_date - timedelta(days=180)
    unused_snapshots = [snapshot for snapshot in snapshots if snapshot['StartTime'].replace(tzinfo=timezone.utc) < six_months_ago and 'VolumeId' in snapshot and snapshot['VolumeId'] in running_instances]
    
    # Delete snapshots
    delete_snapshots(orphan_snapshots + associated_snapshots + unused_snapshots, ec2)

def delete_snapshots(snapshots, ec2):
    for snapshot in snapshots:
        snapshot_id = snapshot['SnapshotId']
        print("Deleting snapshot:", snapshot_id)
        ec2.delete_snapshot(SnapshotId=snapshot_id)
