import boto3
from datetime import datetime
from aws_endpoints_cidr_automate import AWSInfraMapper,GoogleSheetWriter
"""
@author : Apoorv Raj Saxena
@module : AWS Public snapshots Google sheet writer cron

This module conatins AWSSnapshotsRecon class
AWSSnapshotsRecon This modules fetches all the publicaly available snapshots,
explicit permission given to the AWS_key account and current roles private
snapshots.
This module when executed with update the excel sheet using all available
snapshot in all regions
"""
class AWSSnapshotsRecon:
    """
    This class provides two methods for snapshot Recon
    1. get_all_partition_regions - getter for all aws regions
    2. get_snapshots_by_region - getter for all snapshot under given region
    """

    def __init__(self):
        """
        regions which are not supported by default
        https://stackoverflow.com/questions/24744205/ec2-api-error-validating-access-credential
        """
        self.disabled_regions = ['ap-east-1',
                            'cn-north-1',
                            'cn-northwest-1',
                            'us-gov-east-1',
                            'us-gov-west-1',
                            'us-isob-east-1',
                            'us-iso-east-1',
                            'me-south-1']

    def get_all_partition_regions(self,):
        """
        :return returns a list containing all regions under AWS whole Infra
        """
        aws_infra = AWSInfraMapper()
        all_endpoints = aws_infra.get_aws_endpoints()
        partitions = all_endpoints.get('partitions')
        all_regions = [region for partition in partitions for region in partition.get('regions').keys()]
        return all_regions

    def get_snapshots_by_region(self,region):
        """
        :param region for which we need to get the list of snapshots
        :return returns a list of dict containing info of all snapshots under
        given @param:region
        """
        client = boto3.client('ec2',region_name=region)
        return client.describe_snapshots().get('Snapshots')

if __name__=='__main__':
    def run(spreadsheet_url,worksheet_name):
        final_snapshot_list = list()
        snapshot_header = ['region',
                            'snapshot_id',
                            'volume_id',
                            'owner_id',
                            'description',
                            'volume_size',
                            'start_time',
                            'encrypted']
        header_length = len(snapshot_header)
        counter = 1
        aws_snap = AWSSnapshotsRecon()
        all_regions = aws_snap.get_all_partition_regions()
        all_regions = list(set(all_regions) - set(aws_snap.disabled_regions))
        for region in all_regions:
            snapshots = aws_snap.get_snapshots_by_region(region)
            for snapshot in snapshots:
                snapshot_header.extend([region,
                                            snapshot.get('SnapshotId'),
                                            snapshot.get('VolumeId'),
                                            snapshot.get('OwnerId'),
                                            snapshot.get('Description'),
                                            snapshot.get('VolumeSize'),
                                            snapshot.get('StartTime').strftime("%d-%m-%Y"),
                                            snapshot.get('Encrypted')])
                counter = counter +1
        ggl_sheets = GoogleSheetWriter()
        worksheet = ggl_sheets.get_google_sheet_by_url(spreadsheet_url)
        sheet = ggl_sheets.get_new_worksheet(worksheet,worksheet_name)
        ranger = ggl_sheets.get_worksheet_range_param(header_length,counter)
        cell_list = sheet.range(ranger)
        for cell,value in zip(cell_list,snapshot_header):
            cell.value = value
        ggl_sheets.update_worksheet_cells(sheet,cell_list)
    run('https://docs.google.com/spreadsheets/d/1IWausYoqXqxuEMygZXd5wom75d7Lp6HniBw_s2C9XEs/edit?usp=sharing','aws_snapshot_recon')
