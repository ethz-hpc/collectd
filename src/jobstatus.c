/**
 * collectd - src/jobstatus.c
 * Copyright (C) 2015  	    Christiane Pousa
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; only version 2 of the License is applicable.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA  02110-1301 USA
 *
 * Authors:
 *   Christiane Pousa <pousa at id.ethz.ch>
 **/

#include "collectd.h"
#include "common.h"
#include "plugin.h"

#include <unistd.h>

#include <lsf.h>
#include <lsbatch.h>

typedef struct lsf_conf_file
{
  char serverdir[128];	
  char envdir[128];

} lsf_conf_file_t;

static lsf_conf_file_t *lsf_conf = NULL;

static const char *config_keys[] = 
{
	"LSF_SERVERDIR",
	"LSF_ENVDIR",
};

static int config_keys_num = STATIC_ARRAY_SIZE (config_keys);

#define JOB_NAME_LEN 256 

typedef struct  jobstatus
{
	char name[JOB_NAME_LEN];
	unsigned long ncores_pend;
	unsigned long ncores_run;
    unsigned long ndep;
	unsigned long npend;
	unsigned long nrun;
	struct jobstatus *next;
} jobstatus_t;

typedef struct  jobresources
{
    char jobId[JOB_NAME_LEN];
    int reasons;
    int subreasons; 
    unsigned long ncores;
    unsigned long runtime;
    unsigned long memory;
    unsigned long scratch;
    struct jobresources *next;
} jobresources_t;

static jobstatus_t 	*list_head_g = NULL;
static jobresources_t 	*list_head_res_g = NULL;

static void jobstatus_submit_user (jobstatus_t *js)
{
    value_t values[2];
    value_list_t vl = VALUE_LIST_INIT;

    vl.values = values;
    vl.values_len = 2;
    sstrncpy (vl.host, hostname_g, sizeof (vl.host));
    sstrncpy (vl.plugin, "user", sizeof (vl.plugin));
    sstrncpy (vl.plugin_instance, js->name, sizeof (vl.plugin_instance));

    sstrncpy (vl.type, "js_nrun", sizeof (vl.type));
    vl.values[0].gauge = js->nrun;
    vl.values_len = 1;
    plugin_dispatch_values (&vl);

    sstrncpy (vl.type, "js_npend", sizeof (vl.type));
    vl.values[0].gauge = js->npend;
    vl.values_len = 1;
    plugin_dispatch_values (&vl);

    sstrncpy (vl.type, "js_ncores_run", sizeof (vl.type));
    vl.values[0].gauge = js->ncores_run;
    vl.values_len = 1;
    plugin_dispatch_values (&vl);

    sstrncpy (vl.type, "js_ncores_pend", sizeof (vl.type));
    vl.values[0].gauge = js->ncores_pend;
    vl.values_len = 1;
    plugin_dispatch_values (&vl);

    sstrncpy (vl.type, "js_ndep", sizeof (vl.type));
    vl.values[0].gauge = js->ndep;
    vl.values_len = 1;
    plugin_dispatch_values (&vl);

}

static void jobstatus_submit_resources (jobresources_t *js)
{
    value_t values[2];
    value_list_t vl = VALUE_LIST_INIT;

    vl.values = values;
    vl.values_len = 2;
    sstrncpy (vl.host, hostname_g, sizeof (vl.host));
    sstrncpy (vl.plugin, "job", sizeof (vl.plugin));
    sstrncpy (vl.plugin_instance, js->jobId, sizeof (vl.plugin_instance));

    sstrncpy (vl.type, "js_ncores", sizeof (vl.type));
    vl.values[0].gauge = js->ncores;
    vl.values_len = 1;
    plugin_dispatch_values (&vl);

    sstrncpy (vl.type, "js_runtime", sizeof (vl.type));
    vl.values[0].gauge = js->runtime;
    vl.values_len = 1;
    plugin_dispatch_values (&vl);

    sstrncpy (vl.type, "js_memory", sizeof (vl.type));
    vl.values[0].gauge = js->memory;
    vl.values_len = 1;
    plugin_dispatch_values (&vl);

    sstrncpy (vl.type, "js_scratch", sizeof (vl.type));
    vl.values[0].gauge = js->scratch;
    vl.values_len = 1;
    plugin_dispatch_values (&vl);

    sstrncpy (vl.type, "js_reasons", sizeof (vl.type));
    vl.values[0].gauge = js->reasons;
    vl.values_len = 1;
    plugin_dispatch_values (&vl);

    sstrncpy (vl.type, "js_subreasons", sizeof (vl.type));
    vl.values[0].gauge = js->subreasons;
    vl.values_len = 1;
    plugin_dispatch_values (&vl);

}

static void jobstatus_list_add_user (jobstatus_t *js)
{
	jobstatus_t *new;

	new = (jobstatus_t *) malloc (sizeof(jobstatus_t));
    if (new == NULL)
         return;

	memset (new, 0, sizeof (jobstatus_t));

    sstrncpy(new->name, js->name, sizeof(new->name));
    new->nrun = js->nrun;
    new->npend = js->npend;
    new->ncores_run = js->ncores_run;
    new->ncores_pend = js->ncores_pend;
    new->ndep = js->ndep;
	new->next = NULL;

	if (list_head_g == NULL){
		list_head_g = new;
	}	
	else{

		jobstatus_t *ps = list_head_g;
       	jobstatus_t *last_ps;
		while(ps != NULL && strcmp(ps->name,js->name) != 0)
		{
			last_ps = ps;
			ps = ps->next;			
		}	
	
		if (ps == NULL){
			last_ps->next = new;
		}	
		else{
			ps->nrun += new->nrun;
			ps->npend += new->npend;
			ps->ncores_run += new->ncores_run;
			ps->ncores_pend += new->ncores_pend;
            ps->ndep += new->ndep;
		}
	}	
}

static void jobstatus_list_add_res (jobresources_t *js)
{
    jobresources_t *new;

    new = (jobresources_t *) malloc (sizeof(jobresources_t));
    if (new == NULL)
         return;

    memset (new, 0, sizeof (jobresources_t));

    sstrncpy(new->jobId, js->jobId, sizeof(new->jobId));
    new->ncores = js->ncores;
    new->runtime = js->runtime;
    new->memory = js->memory;
    new->scratch = js->scratch;
    new->reasons = js->reasons;
    new->subreasons = js->subreasons;
    new->next = NULL;

    if (list_head_res_g == NULL){
        list_head_res_g = new;
    }
    else{

            jobresources_t *ps = list_head_res_g;
            jobresources_t *last_ps;
            while(ps != NULL && strcmp(ps->jobId,js->jobId) != 0)
            {
                last_ps = ps;
                ps = ps->next;
            }
            if (ps == NULL){
                last_ps->next = new;
            }
            else{
                ps->ncores = new->ncores;
                ps->memory = new->memory;
                ps->scratch = new->scratch;
                ps->runtime = new->runtime;
                ps->reasons = new->reasons;
                ps->subreasons = new->subreasons;
            }
        }
}

static void jobstatus_list_reset (void)
{
    jobstatus_t *ps = list_head_g;
    jobresources_t *ps_res = list_head_res_g;

    while( list_head_res_g!= NULL) {
            ps_res = list_head_res_g;
            list_head_res_g = list_head_res_g->next;
            free(ps_res);
        }	
    
    while( list_head_g!= NULL) {
            ps = list_head_g;
            list_head_g = list_head_g->next;
            free(ps);
        }

	if (lsf_conf != NULL)
		free(lsf_conf);	
}

/*
 * ** Parses LSF's RR string and extracts mem=XXXX
 * ** WARNING: THIS RETURNS MB - MOST OTHER VALUES ARE IN KB
 * */
static int get_rr_mem(char *rr) {
        char match[] = "mem=";      /* prefix to search                         */
        char *scr = NULL;           /* scratch buffer-pointer                   */
        int rmem = -1;              /* return value -> requested memory         */
        int xoff = 0;               /* offset of first integer value after mem= */
        int mlen = strlen(match);
        int rlen = strlen(rr);
        int i;


        for(i=mlen;i<rlen;i++) {
                if(xoff == 0 && memcmp(&rr[i-mlen], match, mlen) == 0) {
                        xoff = i;
                }
                else if(xoff && (rr[i] < '0' || rr[i] > '9')) {
                        break;
                }
        }
        if(xoff) { 
                if( (scr = malloc( 1+i-xoff )) != NULL &&
                       (snprintf(scr, 1+i-xoff, "%s", &rr[xoff])) > 0 ) {
                        rmem = atoi(scr);
                }
        }

        if(scr != NULL)
                free(scr);

        return rmem;
}

/*
 *  * ** Parses LSF's RR string and extracts scratch=XXXX
 *   * ** WARNING: THIS RETURNS MB - MOST OTHER VALUES ARE IN KB
 *    * */
static int get_rr_scratch(char *rr) {
        char match[] = "scratch=";      /* prefix to search                         */
        char *scr = NULL;           /* scratch buffer-pointer                   */
        int rscr = 0;              /* return value -> requested scratch         */
        int xoff = 0;               /* offset of first integer value after scratch= */
        int slen = strlen(match);
        int rlen = strlen(rr);
        int i;

        for(i=slen;i<rlen;i++) {
                if(xoff == 0 && memcmp(&rr[i-slen], match, slen) == 0) {
                        xoff = i;
                }
                else if(xoff && (rr[i] < '0' || rr[i] > '9')) {
                        break;
                }
        }

        if(xoff) { 
                if( (scr = malloc( 1+i-xoff )) != NULL &&
                       (snprintf(scr, 1+i-xoff, "%s", &rr[xoff])) > 0 ) {
                        rscr = atoi(scr);
                }
        }

        if(scr != NULL)
                free(scr);

        return rscr;
}


static jobstatus_t* read_single_job_user (struct jobInfoEnt *job, jobstatus_t *js)
{
    if (job == NULL)
        return NULL;

        sstrncpy(js->name, job->user, sizeof(js->name));

	//TODO: RUN_JOB define can't be used, using for now the value
	if (job->status == 4){
		js->nrun = 1;
		js->npend = 0;
		js->ncores_run = job->numExHosts;
        js->ncores_pend = 0;
    }
	else{
		js->npend = 1;
		js->nrun = 0;
		js->ncores_run = 0;
		js->ncores_pend = job->submit.numProcessors;
    }

	js->next = NULL;
    return js;
}

static jobresources_t* read_single_job_res (struct jobInfoEnt *job, jobresources_t *js)
{
    if (job == NULL)
        return NULL;

    char jobId[JOB_NAME_LEN];

    if (job->status == 4){
	    if(LSB_ARRAY_IDX(job->jobId) > 0){
		    ssnprintf (jobId, sizeof (jobId), "%d.%d", LSB_ARRAY_JOBID(job->jobId),LSB_ARRAY_IDX(job->jobId));
	    } 
	    else
    		ssnprintf (jobId, sizeof (jobId), "%lli", job->jobId);
    	sstrncpy(js->jobId, jobId, sizeof(js->jobId));
        js->ncores = job->submit.numProcessors;
        js->runtime = job->jRusageUpdateTime - job->startTime;
        js->memory = get_rr_mem(job->submit.resReq);
        js->scratch = get_rr_scratch(job->submit.resReq);
        js->reasons = 0;
        js->subreasons = 0;
        js->next = NULL;
    }
	else{
        if(LSB_ARRAY_IDX(job->jobId) > 0){
            ssnprintf (jobId, sizeof (jobId), "%d.%d", LSB_ARRAY_JOBID(job->jobId),LSB_ARRAY_IDX(job->jobId));
        }
        else
            ssnprintf (jobId, sizeof (jobId), "%lli", job->jobId);
        sstrncpy(js->jobId, jobId, sizeof(js->jobId));
        js->ncores = job->submit.numProcessors;
        js->runtime = 0;
        js->memory = get_rr_mem(job->submit.resReq);
        js->scratch = get_rr_scratch(job->submit.resReq);
        js->reasons = job->reasons;
        js->subreasons = job->subreasons;
        js->next = NULL;
	}

    return js;
}


static int add_lsf_conf(const char *key, const char *value)
{
	lsf_conf = (lsf_conf_file_t *) malloc (sizeof(lsf_conf_file_t));
	
	 memset (lsf_conf, 0, sizeof (lsf_conf_file_t));

 	if ((strcasecmp (key, "LSF_SERVERDIR") == 0))
    {
	    sstrncpy(lsf_conf->serverdir,value, sizeof(lsf_conf->serverdir));

	

            if ( lsf_conf->serverdir == NULL)
                        return (-1);
	}	
	else if ((strcasecmp (key, "LSF_ENVDIR") == 0))
    {
            sstrncpy(lsf_conf->envdir, value, sizeof(lsf_conf->envdir));
            if ( lsf_conf->envdir == NULL)
                        return (-1);
        }

	putenv(lsf_conf->envdir);
	putenv(lsf_conf->serverdir);
	return 0;
}

static int jobstatus_config (const char *key, const char *value)
{
	
	if ((strcasecmp (key, "LSF_SERVERDIR") == 0))
	{
		add_lsf_conf(key, value);
	}
	else if (strcasecmp (key, "LSF_ENVDIR") == 0)
	{
		add_lsf_conf(key, value);
	}
	else
		return (-1);

	return (0);
}

static int init(void)
{
	//If appName is NULL, the logfile $LSF_LOGDIR/bcmd receives LSBLIB transaction
	if (lsb_init(NULL) < 0)
	{
		ERROR("Jobstatus plugin: Initialization problems");
		return (-1);
	}

	return 0;
}

static int jobstatus_read (void)
{
	jobstatus_t *js_user;	
	jobresources_t *js_res;

	//jobs state
	int jopts = 0;
	// we only want the running and pending job
	jopts |= RUN_JOB;
	jopts |= PEND_JOB;

	char jobuser[] = "all";
	
	jobstatus_list_reset();

	struct jobInfoHead *jInfoH;
	struct jobInfoEnt *job;	


    if (lsb_init(NULL) < 0)
	{
		ERROR ("jobstatus plugin: Could not start connection com LSF master");
	}

	jInfoH = lsb_openjobinfo_a(0, NULL, jobuser, NULL, NULL, jopts);

	if(jInfoH == NULL){
		lsb_closejobinfo();
	        ERROR ("jobstatus plugin: Could not read job information");
		return (-1);
 	}

   	int i;	

	for(i = 0; i < jInfoH->numJobs; i++) {
        	job = lsb_readjobinfo(NULL);

        	if(job == NULL){
			ERROR ("jobstatus plugin: Could not read job information");
			return (-1);
		}

		js_user = (jobstatus_t *) malloc (sizeof(jobstatus_t));
		if (js_user == NULL)
			return (-1);		

		if ( read_single_job_user(job, js_user) == NULL )
		{
			memset(js_user->name,' ',sizeof(js_user->name));
			js_user->nrun = -1;
			js_user->npend = -1;
			js_user->ncores_run = -1;
			js_user->ncores_pend = -1;
            js_user->ndep = -1;
		}
		else{
			jobstatus_list_add_user(js_user);
		} 

       	if (job->status == 4){
            		struct jobDepRequest jobdepReq;
            		jobdepReq.jobId = job->jobId;
            		jobdepReq.options = QUERY_DEPEND_UNSATISFIED;
            		struct jobDependInfo *jobDep = lsb_getjobdepinfo(&jobdepReq); 
            		js_user->ndep = jobDep->numJobs;
        	}
        	else
            		js_user->ndep = 0;
	
		js_res = (jobresources_t *) malloc (sizeof(jobresources_t));
                if (js_res == NULL)
                        return (-1);

		if ( read_single_job_res(job, js_res) == NULL )
                {
                    memset(js_res->jobId,' ',sizeof(js_res->jobId));
                    js_res->ncores = 0;
                	js_res->runtime = 0;
                	js_res->memory = 0;
                    js_res->scratch = 0;
                    js_res->reasons = 0;
                    js_res->subreasons = 0;
                }
		else{	
			jobstatus_list_add_res(js_res);
		}

	}		

	lsb_closejobinfo();
	for (js_user=list_head_g; js_user != NULL; js_user=js_user->next)
		jobstatus_submit_user(js_user);

	for (js_res=list_head_res_g; js_res != NULL; js_res=js_res->next)
                jobstatus_submit_resources(js_res);

	return (0);
}

void module_register (void)
{
	plugin_register_config ("jobstatus", jobstatus_config, config_keys, config_keys_num);
	plugin_register_init ("jobstatus", init);
	plugin_register_read ("jobstatus", jobstatus_read);
} /* void module_register */
