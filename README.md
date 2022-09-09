# rocks_analysis_pipeline

TLDR: This repo contains scripts for running katydid on the CENPA cluster (rocks) and then conducting the python post processing.

## Notes as I build it: 

* Currently the following works: 
	* To run katydid on a list of run_ids use the following: 
		* Log on to rocks. 
		* `cd /data/eliza4/he6_cres`
		* `./rocks_analysis_pipeline/qsub_katydid.py -rids 440 439 438 -b "2-12_dbscan_high_energy.yaml" -fn 10`
			* The above will run at most fn files for each run_id listed using the base config file provided. 

	* Let this run (perhaps overnight) and then run the following clean-up script. Say the analysis_id assigned to the above katydid run was 009, then you will do the following to clean up that run. The same log files as above will be written to. 
		* Log on to rocks. 
		* `cd /data/eliza4/he6_cres`
		* `./rocks_analysis_pipeline/qsub_katydid.py -rids 440 439 438 -b "2-12_dbscan_high_energy.yaml" -aid 9`
			* The above will rerun all of the files in analysis_id 9 that haven't been created. 

