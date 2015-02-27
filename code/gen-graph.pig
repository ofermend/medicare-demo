
SET  pig.tmpfilecompression true
SET  pig.tmpfilecompression.codec gzip
SET  pig.cachedbag.memusage 0.5

register 'code/udfs.py' using org.apache.pig.scripting.jython.JythonScriptEngine as udfs;

RAW = load '$folder/raw_data' using PigStorage('\t') as (
		npi: chararray, specialty: chararray, 
		hcpcs_code: chararray, hcpcs_description: chararray, bene_day_srvc_cnt: int);
DATA = foreach RAW generate npi, specialty, hcpcs_code as cpt, hcpcs_description as cpt_desc, bene_day_srvc_cnt as count;

-- generate speciality file
SP1 = foreach DATA generate specialty;
SP2 = filter SP1 by specialty != '';
SP3 = distinct SP2 parallel 5;
SP4 = rank SP3 by * ASC DENSE;
SPECIALTY = foreach SP4 generate $1 as sp_name, (int)$0-1 as sp_index;
rmf $folder/specialty
store SPECIALTY into '$folder/specialty' using PigStorage('\t');

-- generate CPT file
CPT1 = foreach DATA generate cpt, cpt_desc;
CPT2 = filter CPT1 by cpt != '';
CPT3 = distinct CPT2 parallel 5;
CPT4 = rank CPT3 by * ASC DENSE;
HCPCS = foreach CPT4 generate $1 as cpt_code, $2 as cpt_desc, (int)$0-1 as cpt_index;
rmf $folder/hcpcs-code
store HCPCS into '$folder/hcpcs-code' using PigStorage('\t');

-- generate NPI mapping file
NPI1 = foreach DATA generate npi;
NPI2 = filter NPI1 by (npi != '');
NPI3 = distinct NPI2 parallel 10;
NPI4 = rank NPI3 by * ASC DENSE;
NPI_MAPPING = foreach NPI4 generate $1 as npi, (int)$0-1 as npi_index;
rmf $folder/npi-mapping
store NPI_MAPPING into '$folder/npi-mapping' using PigStorage('\t');

-- generate final dataset with npi, specialty, CPT and count
DATA0 = filter DATA by NOT(cpt_desc MATCHES '.*[Oo]ffice/outpatient visit.*');	   -- remove 'too common' CPT for regular office visit
DATA1 = join DATA0 by npi, NPI_MAPPING by npi using 'replicated';
DATA2 = join DATA1 by specialty, SPECIALTY by sp_name using 'replicated';
DATA3 = join DATA2 by cpt, HCPCS by cpt_code using 'replicated';
DATA4 = foreach DATA3 generate npi_index, sp_index, cpt_index, count;
NPI_CPT_CODE = foreach (group DATA4 by (npi_index, sp_index, cpt_index) parallel 20) generate 
																				 group.npi_index as npi, 
																				 group.sp_index as specialty, 
																				 group.cpt_index as cpt_inx, 
																				 (int)SUM(DATA4.count) as count;
rmf $folder/npi-cpt-code
store NPI_CPT_CODE into '$folder/npi-cpt-code' using PigStorage('\t'); 

-- Filter out noisy CPT codes and noise NPIs
CODE_GRP = group NPI_CPT_CODE by cpt_inx parallel 20;
CNT_PER_CODE = foreach CODE_GRP generate group as cpt_inx, SUM($1.count) as cpt_total;
VALID_CODES = filter CNT_PER_CODE by cpt_total <= 10000000;	-- Only keep CPTs where total count <= 10M
JOINED = join NPI_CPT_CODE by cpt_inx, VALID_CODES by cpt_inx using 'replicated';
JOINED_WITH_VALID_CODES = foreach JOINED generate npi, NPI_CPT_CODE::cpt_inx as cpt_inx, count;
NPI_GRP = group JOINED_WITH_VALID_CODES by npi parallel 20;
CNT_PER_NPI = foreach NPI_GRP generate group as npi, COUNT($1) as npi_count;
VALID_NPIS = filter CNT_PER_NPI by npi_count >= 3;
JOINED2 = join JOINED_WITH_VALID_CODES by npi, VALID_NPIS by npi using 'replicated';
DATA = foreach JOINED2 generate VALID_NPIS::npi as npi, (int)JOINED_WITH_VALID_CODES::cpt_inx as cpt_inx, count;

-- Create PTS: set of tuples, for each NPI its vector of CPT codes and associated counts (aka cpt_vec)
GRP = group DATA by npi parallel 10;
PTS = foreach GRP generate group as npi, DATA.(cpt_inx, count) as cpt_vec;

-- GROUP PTS into clusters keyed by top shared CPT
PTS_TOP = foreach PTS generate npi, cpt_vec, FLATTEN(udfs.top_cpt(cpt_vec)) as (cpt_inx: int, count: int);
PTS_TOP_CPT = foreach PTS_TOP generate npi, cpt_vec, cpt_inx;
CPT_CLUST = foreach (group PTS_TOP_CPT by cpt_inx parallel 10) generate PTS_TOP_CPT.(npi, cpt_vec) as clust_bag;

-- Use RANK to associate each cluster with a clust_id
RANKED = RANK CPT_CLUST;
ID_WITH_CLUST = foreach RANKED generate $0 as clust_id, clust_bag;

-- Compute pairs of NPIs that are similar to each other, with a cosine similarity score of 0.85 or higher.
-- We implement a few tricks to optimize performance:
-- 1. Split very long clusters into smaller sub-clusters, with max size of 2000 NPIs in each cluster
-- 2. Re-shuffle clusters using a random number to minimize skew
-- 3. Use 'replicated' join for map-side join
ID_WITH_SMALL_CLUST = foreach ID_WITH_CLUST generate clust_id, FLATTEN(udfs.breakLargeBag(clust_bag, 2000)) as clust_bag;
ID_WITH_SMALL_CLUST_RAND = foreach ID_WITH_SMALL_CLUST generate clust_id, clust_bag, RANDOM() as r;
ID_WITH_SMALL_CLUST_SHUF = foreach (GROUP ID_WITH_SMALL_CLUST_RAND by r parallel 240) generate FLATTEN($1) as (clust_id, clust_bag, r);
NPI_AND_CLUST_ID = foreach ID_WITH_CLUST generate FLATTEN(clust_bag) as (npi: int, cpt_vec), clust_id;
CLUST_JOINED = join ID_WITH_SMALL_CLUST_SHUF by clust_id, NPI_AND_CLUST_ID by clust_id using 'replicated';
PAIRS = foreach CLUST_JOINED generate npi as npi1, FLATTEN(udfs.similarNpi(npi, cpt_vec, clust_bag, 0.85)) as npi2;

-- Remove duplicate pairs
OUT = distinct PAIRS parallel 20;

rmf $folder/graph
store OUT into '$folder/graph' using PigStorage('\t');

