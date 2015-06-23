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
  char *serverdir;	

} lsf_conf_file_t;

static lsf_conf_file_t *lsf_conf = NULL;

static const char *config_keys[] = 
{
	"LSF_ENVDIR",
};

static int config_keys_num = STATIC_ARRAY_SIZE (config_keys);

typedef struct  jobstatus
{
	char name[256];
	unsigned long nrun;
	unsigned long npend;
	unsigned long ncores_run;
	unsigned long ncores_pend;
    unsigned long ndep;
	struct jobstatus *next;
} jobstatus_t;

static jobstatus_t *list_head_g = NULL;

static void jobstatus_submit (jobstatus_t *js)
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

static void jobstatus_list_add (jobstatus_t *js)
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

static void jobstatus_list_reset (void)
{
    jobstatus_t *ps = list_head_g;
    
    while( list_head_g!= NULL) {
            ps = list_head_g;
            list_head_g = list_head_g->next;
            free(ps);
        }
}

static jobstatus_t* read_single_job (struct jobInfoEnt *job, jobstatus_t *js)
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

static int add_lsf_conf(const char *key, const char *value)
{
	lsf_conf = (lsf_conf_file_t *) malloc (sizeof(lsf_conf_file_t));
    memset (lsf_conf, '\0', sizeof (lsf_conf_file_t));

 	if ((strcasecmp (key, "LSF_SERVERDIR") == 0))
    {
	    lsf_conf->serverdir = strdup(value);
            if ( lsf_conf->serverdir == NULL)
                        return (-1);
	}	
	return 0;
}

static int jobstatus_config (const char *key, const char *value)
{
	char *new_val;
	char *fields[2];
  	int fields_num;

	new_val = strdup (value);
  	if (new_val == NULL)
    		return (-1);

	fields_num = strsplit (new_val, fields, STATIC_ARRAY_SIZE (fields));

  	if ((fields_num < 1) || (fields_num > 2))
  	{
    		sfree (new_val);
    		return (-1);
  	}

	if ((strcasecmp (key, "LSF_ENVDIR") == 0))
	{
		if (fields_num != 1)
                {
                   ERROR ("jobstatus plugin: Invalid number of fields for option "
                        "`%s'. Got %i, expected 1.", key, fields_num);
                   return (-1);
                }
		else
		   add_lsf_conf(key, fields[0]);
	}

	return (0);
}

static int init(void)
{
	//If appName is NULL, the logfile $LSF_LOGDIR/bcmd receives LSBLIB transaction
	putenv(lsf_conf->serverdir);
	
	if (lsb_init(NULL) < 0)
	{
		ERROR("Jobstatus plugin: Initialization problems");
		return (-1);
	}

	return 0;
}

static int jobstatus_read (void)
{
	jobstatus_t *js;	

	//jobs state
	int jopts = 0;
	// we only want the running and pending job
	jopts |= RUN_JOB;
	jopts |= PEND_JOB;

	char jobuser[] = "all";
	
	jobstatus_list_reset();

	struct jobInfoHead *jInfoH;
	struct jobInfoEnt *job;	

	//Otherwise LSF can't read the configuration files
	putenv(lsf_conf->serverdir);

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

		js = (jobstatus_t *) malloc (sizeof(jobstatus_t));
		if (js == NULL)
			return (-1);		

		if ( read_single_job(job, js) == NULL )
		{
			memset(js->name,' ',sizeof(js->name));
			js->nrun = -1;
			js->npend = -1;
			js->ncores_run = -1;
			js->ncores_pend = -1;
            js->ndep = -1;
		}
        
        if (job->status == 4){
            struct jobDepRequest jobdepReq;
            jobdepReq.jobId = job->jobId;
            jobdepReq.options = QUERY_DEPEND_UNSATISFIED;
            struct jobDependInfo *jobDep = lsb_getjobdepinfo(&jobdepReq); 
            js->ndep = jobDep->numJobs;
        }
        else
            js->ndep = 0;

		jobstatus_list_add(js);
	}		

	for (js=list_head_g; js != NULL; js=js->next)
		jobstatus_submit(js);

	return (0);
}

void module_register (void)
{
	plugin_register_config ("jobstatus", jobstatus_config, config_keys, config_keys_num);
	plugin_register_init ("jobstatus", init);
	plugin_register_read ("jobstatus", jobstatus_read);
} /* void module_register */
