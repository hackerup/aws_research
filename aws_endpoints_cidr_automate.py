import json
import requests
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
"""
@author : Apoorv Raj Saxena
@module : AWS endpoints Google sheet writer cron

This module conatins AWSInfraMapper and GoogleSheetWriter Classs
AWSInfraMapper fetches boto3 updated endpoints file and format it for excel
GoogleSheetWriter exposes methods to write in given Google sheet
This module when executed with update the excel sheet using updated aws infra endpoints
## TODO: Create getter function to directly fetch
1. a requested region's CIDR
2. a requested service's CIDR
3. Default endpoint of a service
"""

class AWSInfraMapper:
    """
    This class maps AWS endpoints,IPv4 and IPV6 details with its partition, region and service.
    """

    def __init__(self,):
        """
        this fetches AWS ip ranges and exposes URL endpoint of boto3.
        """
        self.aws_endpoints = 'https://raw.githubusercontent.com/boto/botocore/develop/botocore/data/endpoints.json'
        ip_ranges = 'https://ip-ranges.amazonaws.com/ip-ranges.json'
        self.ip_ranges = self.get_aws_cidr(ip_ranges)

    def get_aws_cidr(self,url):
        """
        HTTP Getter for ips (v4,v6)
        :param url - endpoint of aws ip ranges json
        :return returns json object with ipv4,v6 prefixes
        """
        ip_ranges = requests.get(url)
        return ip_ranges.json()

    def get_aws_endpoints(self,):
        """
        HTTP Getter for AWS endpoints
        :return - returns json with partitions and regions service info
        """
        endpoint_data = requests.get(self.aws_endpoints)
        return endpoint_data.json()

    def get_aws_partition_defaults(self,partition):
        """
        Getter for a partition defaults values so that region can
        fallback to these values, if values are not available.
        It extracts defaults and set spaced string for G-sheets sake
        :param partition - one of the five partition currently exists
        :return returns a tuple containing all the defaults
        """
        dnsSuffix = partition.get('dnsSuffix',' ')
        services = partition.get('services',' ')
        regions = partition.get('regions',' ')
        regionRegex = partition.get('regionRegex',' ')
        partition_code = partition.get('partition',' ')
        partitionName = partition.get('partitionName',' ')
        defaults = partition.get('defaults',{})
        default_hostname = defaults.get('hostname',' ')
        default_signatureVersions = defaults.get('signatureVersions',' ')
        defaults_protocols = defaults.get('protocols',[' '])[0]
        partition_name = '{} ({})'.format(partition_code,partitionName)
        return (dnsSuffix,services,regions,regionRegex,
                partition_name,default_hostname,
                default_signatureVersions,defaults_protocols)


    def get_aws_endpoint_details(self,endpoint_details,bkp_hst,bkp_sgn,bkp_ptcl):
        """
        Getter for services endpoints details ex. AWS(standard partion)->RDS
        service->ap-south-1->{Details}
        :param endpoint_details - endpoint details JSON Object for value extraction
        :param bkp_hst - backup hostname to fallback to
        :param bkp_sgn - backup signatureVersions to fallback to
        :param bkp_ptcl - backup protocol to fallback to
        :return returns  a tuple containing service endpoint specific details
        """
        cred_s = endpoint_details.get('credentialScope',{}).get('service')
        cred_r = endpoint_details.get('credentialScope',{}).get('region')
        cred_scope = 'service.'+cred_s if cred_s else 'region.'+cred_r if cred_r else ' '
        hostname = endpoint_details.get('hostname',' ') if endpoint_details.get('hostname') else bkp_hst
        protocols = endpoint_details.get('protocols',[' '])[0] if endpoint_details.get('protocols',[''])[0] else bkp_ptcl
        sslCommonName = endpoint_details.get('sslCommonName',' ')
        signatureVersions = endpoint_details.get('signatureVersions',' ') if endpoint_details.get('signatureVersions') else bkp_sgn
        signatureVersions = ' , '.join(signatureVersions)
        return (cred_scope,hostname,protocols,
                sslCommonName,signatureVersions)

    def get_aws_region_ipv_4_cidr(self,region):
        """
        Getter for ipv4 cidr from AWS ip range json with given region
        :param region - region for which IPv4 prefixes needed
        :return It returns string in {region/service}_{ip/cidr} where region/service can be amazon,rds,dynamo etc
        """
        ips =  filter(lambda x:x.get('region')==region,
                        self.ip_ranges.get('prefixes'))
        ips = [x.get('service')+'_'+x.get('ip_prefix') for x in ips]
        ips = ' , '.join(ips)
        return ips


    def get_aws_region_ipv_6_cidr(self,region):
        """
        Getter for ipv6 cidr from AWS ip range json with given region
        :param region - region for which IPv6 prefixes needed
        :return It returns string in {region/service}_{ip/cidr} where region/service can be amazon,rds,dynamo etc
        """
        ips_6 = filter(lambda x:x.get('region')==region,
                        self.ip_ranges.get('ipv6_prefixes'))
        ips_6 = [x.get('service')+'_'+x.get('ipv6_prefix') for x in ips_6]
        ips_6 = ' , '.join(ips_6)
        return ips_6


class GoogleSheetWriter:
    """
    This module contains helper function which will help in CRUD ops
    for Google sheets
    """

    def __init__(self,):
        """
        Initialization needs environment variable GOOGLE_SERVICE_CREDENTIALS
        to be set as a google service credential file pathself.
        scope feed is to get all the sheets available and auth/drive is to do
        CRUD operations
        ## NOTE: If you this class to access your sheet you need to share your
        sheet with the service_account email
        ex. sheets@resource.iam.gserviceaccount.com
        """
        CRED = os.environ.get('GOOGLE_SERVICE_CREDENTIALS')
        scope = ['https://spreadsheets.google.com/feeds',
                'https://www.googleapis.com/auth/drive']
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
                                                                    CRED,scope)
        self.gc = gspread.authorize(credentials)

    def get_new_google_sheet(self,name):
        """
        Getter for worksheet object creted using name variable
        :param name this will create a google sheet using provided name
        """
        worksheet = self.gc.create(name)
        return worksheet

    def get_google_sheet_by_name(self,name):
        """
        Getter for worksheet object already exists with name `name`
        :param name - google sheet name that already exists
        """
        worksheet = self.gc.open(name)
        return worksheet

    def get_google_sheet_by_url(self,name):
        """
        Getter for spreadsheet object already exists with URL `name`
        :param name - URL for google sheet that already exists
        :return returns google spreadsheet object
        """
        spreadsheet = self.gc.open_by_url(name)
        return spreadsheet

    def get_new_worksheet(self,spredsheet,worksheet):
        """
        Getter for new sheet object under `spreadsheet`
        :param worksheet - to create worksheet under given spredsheet
        :param spreadsheet - Google spreadsheet Object
        :return  returns new worksheet object
        """
        sheet  = worksheet.add_worksheet(title=sheetname, rows="100", cols="20")
        return sheet

    def get_worksheet_by_name(self,spreadsheet,worksheet):
        """
        Getter for worksheet object under `spreadsheet`
        :param worksheet - to get existing worksheet under given spredsheet
        :param spreadsheet - Google spreadsheet Object
        :return  returns new worksheet object

        """
        sheet  = worksheet.worksheet(sheetname)
        return sheet

    def get_worksheet_range_param(self,column_count,row_count):
        """
        Getter function for range string. Range object of gspread works as a matrix
        ex. A1:C7 will return an object containing cell object sequencially
        from A to C and Row 1,7 So to generate range parameter one should know
        the row and column_count becfore hand to fill those cell object and
        patch it to the worksheet
        :param column_count - required no. of columns
        :param row_count - required no. of rows
        :return returns the string that can be used in range method of gspread
        """
        cronological = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
        ranger = 'A1:{}{}'.format(cronological[column_count-1],row_count)
        return ranger

    def update_worksheet_cells(self,worksheet,cell_list):
        """
        Setter function updats all the cell iterating over cell_list
        in given worksheet
        :param worksheet - worksheet that needs to be updated
        :param cell_list - list of cell objects conatining value for those cells
        :return It's a setter function doesn't return anything
        """
        worksheet.update_cells(cell_list)


if __name__ == '__main__':

    def run(spreadsheet_url,worksheet_name):
        sheet_header = ['partition','service','region','dnsSuffix','regionRegex',
                        'credentialScope','hostname','protocols','sslCommonName',
                        'signatureVersions','service_IPv4','service_IPV6']
        row_count = 1
        column_count = len(sheet_header)
        infra = AWSInfraMapper()
        ggl_sheets = GoogleSheetWriter()
        all_endpoints = infra.get_aws_endpoints()
        partitions = all_endpoints.get('partitions')
        for partition in partitions:
            (dnsSuffix,services,regions,regionRegex,partition_name,
            default_hostname,default_signatureVersions,
            defaults_protocols) = infra.get_aws_partition_defaults(partition)
            for service,endpoints in services.items():
                for region,endpoint_details in endpoints.get('endpoints').items():
                    (cred_scope,hostname,protocols,sslCommonName,
                    signatureVersions) = infra.get_aws_endpoint_details(endpoint_details,
                                                default_hostname,default_signatureVersions,
                                                defaults_protocols)
                    ipv4 = infra.get_aws_region_ipv_4_cidr(region)
                    ipv6 = infra.get_aws_region_ipv_6_cidr(region)
                    sheet_header.extend([partition_name,
                                        service,
                                        region,
                                        dnsSuffix,
                                        regionRegex,
                                        cred_scope,
                                        hostname,
                                        protocols,
                                        sslCommonName,
                                        signatureVersions,
                                        ipv4,
                                        ipv6])
                    row_count = row_count +1
        # spreadsheet_name = "aws_research"
        # worksheet_name = "aws_data"
        worksheet = ggl_sheets.get_google_sheet_by_url(spreadsheet_url)
        #"https://docs.google.com/spreadsheets/d/1IWausYoqXqxuEMygZXd5wom75d7Lp6HniBw_s2C9XEs/edit?usp=sharing")
        #sheet  = ggl_sheets.get_worksheet_by_name(worksheet,"Recon")
        sheet = ggl_sheets.get_new_worksheet(worksheet,worksheet_name)
        ranger = ggl_sheets.get_worksheet_range_param(column_count,row_count)
        cell_list = sheet.range(ranger)
        for cell,value in zip(cell_list,sheet_header):
            cell.value = value
        ggl_sheets.update_worksheet_cells(sheet,cell_list)

    run("https://docs.google.com/spreadsheets/d/1IWausYoqXqxuEMygZXd5wom75d7Lp6HniBw_s2C9XEs/edit?usp=sharing",
        "aws_data")
