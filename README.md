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

	* Gather the root file output and clean and extract events: 
		* `./rocks_analysis_pipeline/collect_experiment_tracks.py -rids 440 439 438 437 436 434 433 432 431 430 428 427 426 425 424 422 421 420 419 418 416 415 414 413 412 411 409 408 407 406 405 403 402 401 400 399 397 396 395 394 393 391 390 389 388 387 385 384 383 382 381 380 377 376 375 374 373 -aid 13 -name "TEST2"`
		* DOESN'T WORK SUPER WELL RN. 


