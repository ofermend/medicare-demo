import pandas as pd
import numpy as np
import sys
import os
import fnmatch

# Read npidata file
npifile = fnmatch.filter(os.listdir('ref_data'), 'npidata*[!FileHeader].csv')[0]
NPIDATA = pd.read_csv('ref_data/' + npifile, sep=',', quotechar='"', quoting=1, header=0, dtype=str, usecols = ['NPI','Entity Type Code', 'Healthcare Provider Taxonomy Code_1', 'Healthcare Provider Taxonomy Code_2', 'Healthcare Provider Taxonomy Code_3'])
NPIDATA.columns = ['npi', 'entity_type', 'sp1_code', 'sp2_code', 'sp3_code']

# Read NUCC provider taxonomy codes
TAXONOMY = pd.read_csv('ref_data/nucc_taxonomy_150.csv', sep=',', header=0, usecols = ['Code', 'Classification'])
TAXONOMY.columns = ['sp_code', 'specialty']
tax_map = dict(zip(TAXONOMY.sp_code, TAXONOMY.specialty))

def count_specialties(sp_str):
	return len(set(filter(None, sp_str.split(','))))

# map NPI to taxonomy description levels (classification, specialization)
NPIDATA['specialty'] = NPIDATA['sp1_code'].map(tax_map)
NPIDATA['sp_str'] = NPIDATA['sp1_code'].map(tax_map).fillna('') + ',' + NPIDATA['sp2_code'].map(tax_map).fillna('') + ',' + NPIDATA['sp3_code'].map(tax_map).fillna('')
NPIDATA['num_sp'] = NPIDATA['sp_str'].map(count_specialties)
MAPPING = NPIDATA[['npi', 'specialty', 'num_sp', 'entity_type']]

# Add classification and specialization codes to providers in CMS medicare-B dataset
DATASET = pd.read_csv('ref_data/Medicare-Physician-and-Other-Supplier-PUF-CY2012.txt', sep='\t', header=0, skiprows=[1], dtype=str, usecols=['npi', 'provider_type', 'hcpcs_code', 'hcpcs_description', 'bene_day_srvc_cnt'])
JOINED = pd.merge(DATASET, MAPPING, how='left', left_on = 'npi', right_on = 'npi');
FILTERED = JOINED[(JOINED['entity_type']=='1') & (JOINED['num_sp']==1)]		# Only use individuals (no organizations) with a single specialty
RAW_DATA = FILTERED[['npi', 'specialty', 'hcpcs_code', 'hcpcs_description', 'bene_day_srvc_cnt']]

# Store resulting dataset, with only necessary fields and without header
out_file = sys.argv[1]
if len(sys.argv)>2 and float(sys.argv[2])<100.0:
	npis = RAW_DATA['npi'].unique()
	num = int(float(sys.argv[2]) / 100.0 * npis.shape[0])
	np.random.seed(1)
	chosen_npis = np.random.choice(npis, num, replace=False)
	RAW_DATA = RAW_DATA[RAW_DATA['npi'].isin(chosen_npis)]

RAW_DATA.to_csv(out_file, sep='\t', index=False, header=None)

