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

#define JOB_NAME_LEN 256 

typedef struct  jobresources
{
    char jobId[JOB_NAME_LEN];
	unsigned long ncores;
	unsigned long runtime;
    unsigned long memory;
    unsigned long scratch;
	struct jobresources *next;
} jobresources_t;

static jobresources_t *list_head_g = NULL;

static void jobresources_submit (jobresources_t *js)
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
}

static void jobresources_list_add (jobresources_t *js)
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
	new->next = NULL;

	if (list_head_g == NULL){
		list_head_g = new;
	}	
	else{

		jobresources_t *ps = list_head_g;
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
            ps->scratch = ps->scratch;
            ps->runtime = new->runtime;
		}
	}	
}

static void jobresources_list_reset (void)
{
    jobresources_t *ps = list_head_g;
    
    while( list_head_g!= NULL) {
            ps = list_head_g;
            list_head_g = list_head_g->next;
            free(ps);
        }
}

/*
** Parses LSF's RR string and extracts mem=XXXX
** WARNING: THIS RETURNS MB - MOST OTHER VALUES ARE IN KB
*/
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
        if(xoff) { /* hit is between i <-> xoff */
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
 * ** Parses LSF's RR string and extracts scratch=XXXX
 * ** WARNING: THIS RETURNS MB - MOST OTHER VALUES ARE IN KB
 * */
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

        if(xoff) { /* hit is between i <-> xoff */
                if( (scr = malloc( 1+i-xoff )) != NULL &&
                       (snprintf(scr, 1+i-xoff, "%s", &rr[xoff])) > 0 ) {
                        rscr = atoi(scr);
                }
        }

        if(scr != NULL)
                free(scr);

        return rscr;
}

static jobresources_t* read_single_job (struct jobInfoEnt *job, jobresources_t *js)
{
    if (job == NULL)
        return NULL;

    char jobId[JOB_NAME_LEN];

    ssnprintf (jobId, sizeof (jobId), "%lli", job->jobId); 
    sstrncpy(js->jobId, jobId, sizeof(js->jobId));

	//TODO: RUN_JOB define can't be used, using for now the value
	if (job->status == 4){
		js->ncores = job->submit.numProcessors;
        js->runtime = job->jRusageUpdateTime - job->startTime;
        js->memory = get_rr_mem(job->submit.resReq);
        js->scratch = get_rr_scratch(job->submit.resReq);    
    }

	js->next = NULL;

    return js;
}

static int add_lsf_conf(const char *key, const char *value)
{
	lsf_conf = (lsf_conf_file_t *) malloc (sizeof(lsf_conf_file_t));
    memset (lsf_conf, '\0', sizeof (lsf_conf_file_t));

 	if ((strcasecmp (key, "LSF_ENVDIR") == 0))
    {
	    lsf_conf->serverdir = strdup(value);
            if ( lsf_conf->serverdir == NULL)
                        return (-1);
	}	
	return 0;
}

static int jobresources_config (const char *key, const char *value)
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
		ERROR("Jobresources plugin: Initialization problems");
		return (-1);
	}

	return 0;
}

static int jobresources_read (void)
{
	jobresources_t *js;	

	//jobs state
	int jopts = 0;
	// we only want the running and pending job
	jopts |= RUN_JOB;

	char jobuser[] = "all";
	
	jobresources_list_reset();

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

		js = (jobresources_t *) malloc (sizeof(jobresources_t));
		if (js == NULL)
			return (-1);		

		if ( read_single_job(job, js) == NULL )
		{
			memset(js->jobId,' ',sizeof(js->jobId));
			js->ncores = 0;
            js->runtime = 0;
            js->memory = 0;
            js->scratch = 0;
		}
        else 
		    jobresources_list_add(js);
	}		

	for (js=list_head_g; js != NULL; js=js->next)
		jobresources_submit(js);

	return (0);
}

void module_register (void)
{
	plugin_register_config ("jobresources", jobresources_config, config_keys, config_keys_num);
	plugin_register_init ("jobresources", init);
	plugin_register_read ("jobresources", jobresources_read);
} /* void module_register */
