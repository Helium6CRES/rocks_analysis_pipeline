
<p align="center"><img width="30%" src="/demo/readme_imgs/he6-cres_logo.png" /></p>

--------------------------------------------------------------------------------
# rocks_analysis_pipeline

This repo contains scripts for running katydid, a C++ based analysis tool adapted from Project 8 that extracts physically relevant features from spectrograms, on the CENPA cluster (WULF) and then conducts the post processing of these tracks and events.

--------------------------------------------------------------------------------
### Run an analysis then make interactive plots of cres track features!

#### Look at track and event classification overlaid on top of raw data: 

<p align="center"><img width="42%" src="/demo/readme_imgs/sparse_spec.png" />              <img width="42%" src="/demo/readme_imgs/track_overlay.png" /><img width="80%" src="/demo/readme_imgs/event_overlay.png" /></p>

#### Inertactively look at relationships between extracted cres event features: 

<p align="center"><img width="19%" src="/demo/readme_imgs/display_options_1.png" />              <img width="73%" src="/demo/readme_imgs/scatter_plot_0.png" /></p>

<p align="center"><img width="19%" src="/demo/readme_imgs/display_options_2.png" />              <img width="73%" src="/demo/readme_imgs/scatter_plot_1.png" /></p>

<p align="center"><img width="30%" src="/demo/readme_imgs/display_options_3.png" />              <img width="60%" src="/demo/readme_imgs/scatter_plot_2.png" /></p>

--------------------------------------------------------------------------------

## Instructions for running an analysis on WULF (CENPA compute cluster): 


### Get set up on WULF: 
To facilitate developers having the same environment across machines and on cluster nodes, we need to use a containerized development environemnt. This is done with apptainer. The container is built to be totally independant of previous projects, such as project8, to support ROOT, Katydid, and include all the base python dependancies used mainly for analysis on the CENPA Wulf cluster. The container is deffined via the apptainer definition file, he6cres-base.def which builds everything from ubuntu:20.04 to make a reproducible He6-CRES environment with Python 3.7.3, pip dependencies, and ROOT 6.22/06. This definition file is used to buld the container: he6cres-base.sif which can be used by anyone. \
In normal operations, developers should just use the existing .sif file as is while changing katydid or analysis scripts for example for the beta monitor.

The container can be found at: \
`/data/raid2/eliza4/he6_cres/containers/he6cres-base.sif`

To enter an interactive appt session, run \
`apptainer shell --bind /data/raid2/eliza4/he6_cres /data/raid2/eliza4/he6_cres/containers/he6cres-base.sif`

Here you can, for example, check which python version is in the container: \
`which python3`
`python3`

With the container, after getting an account on WULF, you should be all set to go as long as you run jobs within the container. Most scripts are already configured to do this when submitting jobs to nodes. If you run anything on the head node outside the container, note that you will not have any dependancies installed.

### Update katydid on ROCKS:
* cd into katydid directory, stash existing version and run 
    * $ `git pull origin feature/FreqDomainInput`
* Check permissions. Go back to /data/raid2eliza4/he6_cres/ and run 
    * $ `chmod -R 777 katydid`
* Enter apptainer and bind the local file system on the wulf head node.
    * $ `apptainer shell --bind /data/raid2/eliza4/he6_cres /data/raid2/eliza4/he6_cres/containers/he6cres-base.sif`
* source root from prebuilt ROOT tarball in container acessable. Updates the current shell’s environment. Very important!:
    * $ `source /usr/local/root/bin/thisroot.sh`
* Then compile
    * > `cd katydid/build`
    * > `cmake .. -DCMAKE_BUILD_TYPE=RELEASE -DUSE_CPP14=ON -DKatydid_USE_MATLAB=OFF`
    * > `make` 
    * > `make install`
* Then exit singularity, from he6_cres copy over new config gile to base_configs:
    * $ `cp katydid/Examples/ConfigFiles/2-12_LTF_MBEB_tausnr7_2400.yaml katydid_analysis/base_configs/`
* and set it's permisissions
    * $ `chmod 774 katydid_analysis/base_configs/2-12_LTF_MBEB_tausnr7_2400.yaml`
	
### Run katydid:

* **Overview:** Run katydid on a list of run_ids.
* **Step 0:** Run katydid for the first time on a list of run_ids: 
	* Log on to rocks. 
	* `cd /data/raid2/eliza4/he6_cres`
	* `./rocks_analysis_pipeline/sbatch_katydid.py -rids 1748 1749 1750 1751 1752 1753 1754 1757 1758 1759 1760 1761 1762 1763 1767 1768 1769 1770 1771 1772 1773 1775 1776 1777 1778 1779 1780 1781 1784 1785 1786 1787 1788 1789 1790 1791 1795 1796 1797 1798 1799 1800 1801 1804 1805 1806 1807 1808 1809 1810 1813 1814 1815 1816 1817 1818 1819 1821 1822 1823 1824 1825 1826 1827 1829 1830 1831 1832 1833 1834 1835 1843 1844 1845 1846 1847 1848 1849 1853 1854 1855 1856 1857 1858 1859 1863 1864 1865 1866 1867 1868 1869 1874 1875 1876 1877 1878 1879 1880 1885 1886 1887 1888 1889 1890 1891 1899 1900 1901 1902 1903 1904 1905 -nid 1716 -b "2-12_LTF_MBEB_tausnr7_2400.yaml" -fn 1000`
		* The above will run at most fn files for each run_id listed using the base config file provided. 
		* For reference the above jobs (one job per run_id) were mostly finished in 30 mins. 
		* A analysis_id (aid) will be assigned to the analysis. Example: aid = 9.
		* A job log for each run_id will be created. Example: rid_1801_009.txt

* **Step 1:** Clean up. Let the above run (perhaps overnight) and then run the following clean-up script. Say the analysis_id assigned to the above katydid run was 009, then you will do the following to clean up that run. The same log files as above will be written to. Best to run the below twice if doing an analysis that has many many run_ids/spec files (greater than 500 files or so).
	* Log on to rocks. 
	* `cd /data/raid2/eliza4/he6_cres`
	* `./rocks_analysis_pipeline/sbatch_katydid.py -rids 1748 1749 1750 1751 1752 1753 1754 1757 1758 1759 1760 1761 1762 1763 1767 1768 1769 1770 1771 1772 1773 1775 1776 1777 1778 1779 1780 1781 1784 1785 1786 1787 1788 1789 1790 1791 1795 1796 1797 1798 1799 1800 1801 1804 1805 1806 1807 1808 1809 1810 1813 1814 1815 1816 1817 1818 1819 1821 1822 1823 1824 1825 1826 1827 1829 1830 1831 1832 1833 1834 1835 1843 1844 1845 1846 1847 1848 1849 1853 1854 1855 1856 1857 1858 1859 1863 1864 1865 1866 1867 1868 1869 1874 1875 1876 1877 1878 1879 1880 1885 1886 1887 1888 1889 1890 1891 1899 1900 1901 1902 1903 1904 1905 -nid 1716 -b "2-12_LTF_MBEB_tausnr7_2400.yaml" -fn 1000 -aid 9`
		* The above will rerun all of the files in analysis_id 9 that haven't yet been created.
		* Note that you want to include "-fn 3" here in case a node failed before even creating the  

### Post Processing:

First if you want to use ude the offline beta monitor counting, run this on each rid with
`./rocks_analysis_pipeline/sbatch_count_offline_mon_rates.py -rids 1748 1749 1750 1751 1752 1753 1754 1757 1758 1759 1760 1761 1762 1763 1767 1768 1769 1770 1771 1772 1773 1775 1776 1777 1778 1779 1780 1781 1784 1785 1786 1787 1788 1789 1790 1791 1795 1796 1797 1798 1799 1800 1801 1804 1805 1806 1807 1808 1809 1810 1813 1814 1815 1816 1817 1818 1819 1821 1822 1823 1824 1825 1826 1827 1829 1830 1831 1832 1833 1834 1835 1843 1844 1845 1846 1847 1848 1849 1853 1854 1855 1856 1857 1858 1859 1863 1864 1865 1866 1867 1868 1869 1874 1875 1876 1877 1878 1879 1880 1885 1886 1887 1888 1889 1890 1891 1899 1900 1901 1902 1903 1904 1905 -aid 9`

This will go rid by rid and add environmental data and the offline monitor counts to the root file csvs rid_df_1801_009.csv and write the output to  -> rid_df_1801_009_with_offline_mon.csv. Leter when you run the track and event post processing, it will check if this was already done. If so, it iwll jsut use these. If not, it will add the environmental data during stage 0

* **Overview:** This is a three stage process. Run each stage without changing anything but the -stage argument. the -ms_standard argument determines the expected
spec(k) file name time format for the data you want to process.
	* 0: Root file names only to second. %Y-%m-%d-%H-%M-%S use for rid 1570 and earlier!
   	* 1: Root file names to ms. "%Y-%m-%d-%H-%M-%S-%f
	* For each of the steps, begin by navigating to our groups directory on eliza4: 
		* Log on to rocks. 
		* `cd /data/raid2/eliza4/he6_cres`
* **Stage 0:** Set-up.  
	* `./rocks_analysis_pipeline/sbatch_post_processing.py -rids 1748 1749 1750 1751 1752 1753 1754 1757 1758 1759 1760 1761 1762 1763 1767 1768 1769 1770 1771 1772 1773 1775 1776 1777 1778 1779 1780 1781 1784 1785 1786 1787 1788 1789 1790 1791 1795 1796 1797 1798 1799 1800 1801 1804 1805 1806 1807 1808 1809 1810 1813 1814 1815 1816 1817 1818 1819 1821 1822 1823 1824 1825 1826 1827 1829 1830 1831 1832 1833 1834 1835 1843 1844 1845 1846 1847 1848 1849 1853 1854 1855 1857 1863 1864 1865 1866 1868 1869 1874 1875 1878 1879 1880 1886 1888 1891 1899 1901 1903 1905 -aid 15 -name "Ne19_Spectrum2025_QSTQWP_1millKp_07272025_LTF2025_MBEB_511cut" -nft 1000 -nfp 5 -ms_standard 1 -stage 0`
		* The above will first build the saved_experiment directory and then collect all of the `root_files.csv` files in the given list of run_ids and gather them into one csv that will be written into the saved_experiment directory ([name]_aid_[aid]).
		* If rid_df_1801_009_with_offline_mon.csv exists, it will use these.
		* Before moving on to stage 1, check to see that the directory was made and the `root_files.csv` is present. 
		* `-ms_standard` flag: if spec file names are in s (0) or ms (1) format.
		* -nft gives number of files in each rid you want tracks for.
		* -nfp gives number of files in each rid you want all points in tracks for.
* **Stage 1:** Processing.  
	* `./rocks_analysis_pipeline/sbatch_post_processing.py -rids 1748 1749 1750 1751 1752 1753 1754 1757 1758 1759 1760 1761 1762 1763 1767 1768 1769 1770 1771 1772 1773 1775 1776 1777 1778 1779 1780 1781 1784 1785 1786 1787 1788 1789 1790 1791 1795 1796 1797 1798 1799 1800 1801 1804 1805 1806 1807 1808 1809 1810 1813 1814 1815 1816 1817 1818 1819 1821 1822 1823 1824 1825 1826 1827 1829 1830 1831 1832 1833 1834 1835 1843 1844 1845 1846 1847 1848 1849 1853 1854 1855 1857 1863 1864 1865 1866 1868 1869 1874 1875 1878 1879 1880 1886 1888 1891 1899 1901 1903 1905 -aid 15 -name "Ne19_Spectrum2025_QSTQWP_1millKp_07272025_LTF2025_MBEB_511cut" -nft 1000 -nfp 5 -ms_standard 1 -stage 1`	
		* This is the meat and potatoes of the post processing. nft files worth of tracks for each run_id, and ntf files worth of tracks for each run_id are written to disk as csvs. In order to allow for this to be done in parallel, each node is handed one file_id and processes all of the files with that file_id across all run_ids. Two files (track_points_[fid].csv, tracks_[fid].csv) are built for each fid. 
		* Before moving on to stage 2, check to see that the directory contains nft tracks and nfe events csvs. 
		* If for some reason (most likely failed nodes) all of the trackss_{n}.csv's aren't created rerun the exact same command. It will detect the missing ones and rerun those. 
		* Note: should change it to add an optional lower number of files (or zero) to output track_points_[fid].csv for as this file is quite large and only useful for some SNR and slope studies 
* **Stage 2:** Clean-up. 
	* `./rocks_analysis_pipeline/sbatch_post_processing.py -rids 1748 1749 1750 1751 1752 1753 1754 1757 1758 1759 1760 1761 1762 1763 1767 1768 1769 1770 1771 1772 1773 1775 1776 1777 1778 1779 1780 1781 1784 1785 1786 1787 1788 1789 1790 1791 1795 1796 1797 1798 1799 1800 1801 1804 1805 1806 1807 1808 1809 1810 1813 1814 1815 1816 1817 1818 1819 1821 1822 1823 1824 1825 1826 1827 1829 1830 1831 1832 1833 1834 1835 1843 1844 1845 1846 1847 1848 1849 1853 1854 1855 1857 1863 1864 1865 1866 1868 1869 1874 1875 1878 1879 1880 1886 1888 1891 1899 1901 1903 1905 -aid 15 -name "Ne19_Spectrum2025_QSTQWP_1millKp_07272025_LTF2025_MBEB_511cut" -nft 1000 -nfp 5 -ms_standard 1 -stage 2`
		* The above will gather all of the track points and tracks csvs (respectively) into one csv. 

### Document your analysis
There is an elog for analyses run on ROCKS. Please see https://maxwell.npl.washington.edu/elog/he6cres/Katydid+analysis/ under our software elog. When you finish running a new analysis as described above, you should document it here. The title should be the "experiment_name" entered in the post-processing, and should contain
* The "experiment name" and who ran the analysis
* A short written summary of the goals indicating what run_ids were used, why this analysis was run, and any issues with it or context that future users might want to know
* A copy of the top output from the post-processing job_log up to where it says Post Processing Stage 0 DONE at PST time: XXX as this contains most of the relevant information including paths to written csv files.
This elog is not currently backfilled from before the first phase-II data campaign. Going forward anyone who runs an analysis should make an elog in this format. I know this is a bit annoying because right now we can't access the elog while on the vpn, so you have to copy info from the job_log and then close the connection to rocks, close the vpn, and then make the elog post. I recomend making a local file and then copying the contents to your elog post when you are off the vpn.

### Tools to investigate event classification quality and to conduct analysis. 

* **Investigate results:**
	* Grab the saved experiment and investigate the quality of the analysis.
	* Download the files locally to your computer to do further analysis. eg.
	* `scp heathh6@wulf.npl.washington.edu:/data/raid2/eliza4/he6_cres/katydid_analysis/saved_experiments/Ne19_Spectrum2025_QSTQWP_1millKp_07272025_LTF2025_MBEB_511cut_aid_15/tracks.csv .`
	* Suggestion is to use `Helium6CRES/coral_reef/heather/Harrington2025Analysis`
	* `LoadData2025` correctly combines 1s environment data and monitor rates from root_files.csv with tracks_[fid].csv
	* Edit `LoadData2025` to apply standardized cuts on tracks, field-wise cuts, and monitor rate cuts.
	* Read in data with the standardized
	`file_summary, rid_summary, field_summary, valid_tracks = LoadData2025.get_valid_tracks_and_mon_norm(exp_name, aid, drop_rids, offline=use_offline_counts, mon_cut=mon_cut)`
	* Produces three summary dataframes and a df of all the remaining valid tracks with additional environmental data added.
	* `Helium6CRES/coral_reef/heather/Harrington2025Analysis` contains example scripts for several specific studies, as well as the GeneralTrakPlotting.py whihc contains gernal plotting functions that I use often.

* **(old) This was Drew's pipeline and has not been maintained. Use at your own risk:**
	* This is to be done locally using `class ExperimentResults` in module `results.py`.
	* A full demo of how this is done is here: `/rocks_analysis_pipeline/demo/rocks_analysis_demo.ipynb`. 
	* Copy the `.ipynb` into your own directory suitable for analysis and give it a try. Make neat plots like the one you see in the top of this readme. 


--------------------------------------------------------------------------------

## Useful stuff: 

* **Slurm:**
	* `sbatch submit_job.sh` Submit a job script
	* `squeue` to see all of the jobs you have running or in the queue.
	* `scancel -u netid` (delete all the jobs of user netid)
	* `scancel 4807` (delete job id 4807)
	* `scontrol show job 466` show info about a job eg jobid 466
	* `scontrol show node n4180` show info about a node

* **Permissions:**
	* I'm finding that with multiple users working in this analysis pipeline simultaneously the permissions can get weird. The following two commands run from `/he6_cres` should help: 
		* `chmod -R 774 katydid_analysis/`
		* `chgrp -R he6_cres katydid_analysis/`

* **Apptainer:**
	* To interactively enter the analysis apptainer run the following: 
		* `apptainer shell --bind /data/raid2/eliza4/he6_cres /data/raid2/eliza4/he6_cres/containers/he6cres-base.sif`
	* To exit the container: 
		* `exit`
		
* **BASH:**
	* Pretty print a csv in bash (useful for sanity checking):
		* `column -s, -t < katydid_analysis/root_files/rid_1856/aid_015/rid_df_1856_015.csv | less -#2 -N -S`
		* `.q` to exit. 
	* Check number of rows in a csv (useful for checking len of df/csv): 
		* `column -s, -t < events.csv | less -#2 -N -S | wc -l`
	* Count number of files in a directory: 
		* `ls -1 | wc -l`

* **Rocks:**
	* To check on rocks use this site (won't work when on the cenpa VPN): 
		* `http://cenpa-rocks.npl.washington.edu/ganglia/?r=hour&cs=&ce=&m=load_one&s=by+name&c=&tab=m&vn=&hide-hf=false`

* **RGA patches:**
	* oh no, you realized that some of your data is missing pressure information? This can be caused by the RGA Flask app using the old log file’s base time (fileUTCstamp) even after the RGA software started a new log. During that period, the DAQ stored zero partial pressures and large time_since_write values (~94 000 s), because the Flask app thought the last line was “nearly a day old.” Once the Flask app was restarted or began reading the new log correctly, everything lined up again — only ~12 s of nominal polling delay between DAQ and RGA. The verification script (`check_rga_sql_consistency.py`) can be used to compare RGA log vs. SQL table showed two regimes:
		* Early entries → Δt ≈ +6.4e4 s
		* Later entries → Δt ≈ +12 s (healthy)
	* A Python patch script (`rga_patch_by_created_at.sql`) was written to:
		* Parse the correct RGA .txt log,
		* Match each bad SQL row (identified by created_at < first good entry) to the most recent valid RGA line,
		* Update all partial-pressure columns, total, utc_write_time, and time_since_write accordingly
	* Hopefully you don't have to use it, but I'm leaving it here as a utility.
--------------------------------------------------------------------------------
