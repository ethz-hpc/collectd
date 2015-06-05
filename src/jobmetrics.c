/**
 * collectd - src/jobmetrics.c
 * Copyright (C) 2015       Christiane Pousa
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or (at your
 * option) any later version.
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
 *   Christiane Pousa < pousa at ethz.ch >
 **/

#include "collectd.h"
#include "common.h"
#include "plugin.h"
#include "configfile.h"


#if KERNEL_LINUX
#  if HAVE_LINUX_CONFIG_H
#    include <linux/config.h>
#  endif
#  ifndef CONFIG_HZ
#    define CONFIG_HZ 100
#  endif

#define MAXPID 65536
/* #endif KERNEL_LINUX */
#endif

#if HAVE_PROCINFO_H
#  include <procinfo.h>
#  include <sys/types.h>

#define MAXPROCENTRY 32
#define MAXTHRDENTRY 16
#define MAXARGLN 1024
/* #endif HAVE_PROCINFO_H */
#endif

#if HAVE_REGEX_H
# include <regex.h>
#endif

#if HAVE_KSTAT_H
# include <kstat.h>
#endif

#ifndef CMDLINE_BUFFER_SIZE
# if defined(ARG_MAX) && (ARG_MAX < 4096)
#  define CMDLINE_BUFFER_SIZE ARG_MAX
# else
#  define CMDLINE_BUFFER_SIZE 4096
# endif
#endif

typedef struct procstat_entry_s
{
	unsigned long id;
	unsigned long age;

	unsigned long num_proc;
	unsigned long num_lwp;
	unsigned long vmem_size;
	unsigned long vmem_rss;
	unsigned long vmem_data;
	unsigned long vmem_code;
	unsigned long stack_size;
    unsigned long voluntary_ctxt_switches;
    unsigned long nonvoluntary_ctxt_switches;

	unsigned long vmem_minflt;
	unsigned long vmem_majflt;
	derive_t      vmem_minflt_counter;
	derive_t      vmem_majflt_counter;

	unsigned long cpu_user;
	unsigned long cpu_system;
	derive_t      cpu_user_counter;
	derive_t      cpu_system_counter;

	/* io data */
	derive_t io_rchar;
	derive_t io_wchar;
	derive_t io_syscr;
	derive_t io_syscw;

	struct procstat_entry_s *next;
} procstat_entry_t;

#define PROCSTAT_NAME_LEN 256
typedef struct procstat
{
	char          name[PROCSTAT_NAME_LEN];
    char          jobId[PROCSTAT_NAME_LEN];
                
#if HAVE_REGEX_H
	regex_t *re;
#endif
	unsigned long num_proc;
	unsigned long num_lwp;
	unsigned long vmem_size;
	unsigned long vmem_rss;
	unsigned long vmem_data;
	unsigned long vmem_code;
	unsigned long stack_size;
    unsigned long voluntary_ctxt_switches;
    unsigned long nonvoluntary_ctxt_switches;

	derive_t vmem_minflt_counter;
	derive_t vmem_majflt_counter;

	derive_t cpu_user_counter;
	derive_t cpu_system_counter;

	/* io data */
	derive_t io_rchar;
	derive_t io_wchar;
	derive_t io_syscr;
	derive_t io_syscw;

	struct procstat   *next;
	struct procstat_entry_s *instances;
} procstat_t;

static procstat_t *list_head_g = NULL;


#if KERNEL_LINUX
static long pagesize_g;
/* #endif KERNEL_LINUX */

#elif HAVE_PROCINFO_H
static  struct procentry64 procentry[MAXPROCENTRY];
static  struct thrdentry64 thrdentry[MAXTHRDENTRY];
static int pagesize;

int getargs (void *processBuffer, int bufferLen, char *argsBuffer, int argsLen);
#endif /* HAVE_PROCINFO_H */


/* try to match jobId against entry, returns 1 if success */
static int jobmetrics_list_match (const char *jobId, const char *name, const char *cmdline, procstat_t *ps)
{
    
#if HAVE_REGEX_H
	if (ps->re != NULL)
	{
		int status;
		const char *str;

		str = cmdline;
		if ((str == NULL) || (str[0] == 0))
			str = jobId;

		assert (str != NULL);

		status = regexec (ps->re, str,
				/* nmatch = */ 0,
				/* pmatch = */ NULL,
				/* eflags = */ 0);
		if (status == 0)
			return (1);
	}
	else
#endif
	if (strcmp (ps->jobId, jobId) == 0 && strcmp (ps->name, name) == 0)
		            return (1);

	return (0);
} /* int jobmetrics_list_match */


/* remove process entry for jobs that have already finished*/
static void jobmetrics_list_remove(const char *jobId)
{
    procstat_t *ps, *ps_ahead, *ps_behind;
    
    if ( list_head_g != NULL)
    {
        ps = list_head_g;
        while (ps != NULL){
            ps_behind = ps;
            if (strcmp(ps->jobId, jobId) == 0)
            {
                ps_ahead = ps->next;
                break;
            }
            ps = ps->next;        
        }
        ps_behind->next = ps_ahead;
        free(ps);
    }
}


/* add process entry to 'instances' of process 'name' (or refresh it) and JOBID jobId*/
static void jobmetrics_list_add (const char *jobId, const char *name, const char *cmdline, procstat_entry_t *entry)
{
	procstat_t *ps;
    procstat_t *new;
    procstat_t *ptr;
	procstat_entry_t *pse;
    int already_added;

	if (entry->id == 0)
		return;

    if ( list_head_g == NULL)
    {
        new = (procstat_t *) malloc (sizeof (procstat_t));
        if (new == NULL)
        {
                ERROR ("jobmetrics plugin: jobmetrics_list_register: malloc failed.");
                return;
        }
        memset (new, 0, sizeof (procstat_t));
        sstrncpy (new->name, name, sizeof (new->name));
        sstrncpy (new->jobId, jobId, sizeof (new->jobId));
        list_head_g = new;
    }
    else
    {
        ps = list_head_g;
        already_added = 0;

        already_added = 0;
        while (ps != NULL)
        {
                if (jobmetrics_list_match (jobId, name, cmdline, ps) != 0)
                { already_added = 1; break;}
                ps = ps->next;
        }

        //no job added with this id and name
        if (! already_added)
        {
            new = (procstat_t *) malloc (sizeof (procstat_t));
            if (new == NULL)
            {
                ERROR ("jobmetrics plugin: jobmetrics_list_register: malloc failed.");
                return;
            }
            memset (new, 0, sizeof (procstat_t));
            sstrncpy (new->name, name, sizeof (new->name));
            sstrncpy (new->jobId, jobId, sizeof (new->jobId));
            for (ptr = list_head_g; ptr != NULL; ptr = ptr->next)
                if (ptr->next == NULL)
                        break;
            ptr->next = new;
        }
    }

	for (ps = list_head_g; ps != NULL; ps = ps->next)
	{

		if ((jobmetrics_list_match (jobId, name, cmdline, ps)) == 0){
			continue;
		}
        
		for (pse = ps->instances; pse != NULL; pse = pse->next)
			if ((pse->id == entry->id) || (pse->next == NULL))
				break;

		if ((pse == NULL) || (pse->id != entry->id))
		{
			procstat_entry_t *new;

			new = (procstat_entry_t *) malloc (sizeof (procstat_entry_t));
			if (new == NULL)
				return;
			memset (new, 0, sizeof (procstat_entry_t));
			new->id = entry->id;

			if (pse == NULL)
				ps->instances = new;
			else
				pse->next = new;

			pse = new;
		}

		pse->age = 0;
		pse->num_proc   = entry->num_proc;
		pse->num_lwp    = entry->num_lwp;
		pse->vmem_size  = entry->vmem_size;
		pse->vmem_rss   = entry->vmem_rss;
		pse->vmem_data  = entry->vmem_data;
		pse->vmem_code  = entry->vmem_code;
		pse->stack_size = entry->stack_size;
		pse->io_rchar   = entry->io_rchar;
		pse->io_wchar   = entry->io_wchar;
		pse->io_syscr   = entry->io_syscr;
		pse->io_syscw   = entry->io_syscw;
        pse->voluntary_ctxt_switches = entry->voluntary_ctxt_switches;
        pse->novoluntary_ctxt_switches = entry->novoluntary_ctxt_switches;

        ps->num_proc   += pse->num_proc;
		ps->num_lwp    += pse->num_lwp;
		ps->vmem_size  += pse->vmem_size;
		ps->vmem_rss   += pse->vmem_rss;
		ps->vmem_data  += pse->vmem_data;
		ps->vmem_code  += pse->vmem_code;
		ps->stack_size += pse->stack_size;

		ps->io_rchar   += ((pse->io_rchar == -1)?0:pse->io_rchar);
		ps->io_wchar   += ((pse->io_wchar == -1)?0:pse->io_wchar);
		ps->io_syscr   += ((pse->io_syscr == -1)?0:pse->io_syscr);
		ps->io_syscw   += ((pse->io_syscw == -1)?0:pse->io_syscw);
    
        ps->voluntary_ctxt_switches += pse->voluntary_ctxt_switches;
        ps->novoluntary_ctxt_switches += pse->novoluntary_ctxt_switches;

		if ((entry->vmem_minflt_counter == 0)
				&& (entry->vmem_majflt_counter == 0))
		{
			pse->vmem_minflt_counter += entry->vmem_minflt;
			pse->vmem_minflt = entry->vmem_minflt;

			pse->vmem_majflt_counter += entry->vmem_majflt;
			pse->vmem_majflt = entry->vmem_majflt;
		}
		else
		{
			if (entry->vmem_minflt_counter < pse->vmem_minflt_counter)
			{
				pse->vmem_minflt = entry->vmem_minflt_counter
					+ (ULONG_MAX - pse->vmem_minflt_counter);
			}
			else
			{
				pse->vmem_minflt = entry->vmem_minflt_counter - pse->vmem_minflt_counter;
			}
			pse->vmem_minflt_counter = entry->vmem_minflt_counter;

			if (entry->vmem_majflt_counter < pse->vmem_majflt_counter)
			{
				pse->vmem_majflt = entry->vmem_majflt_counter
					+ (ULONG_MAX - pse->vmem_majflt_counter);
			}
			else
			{
				pse->vmem_majflt = entry->vmem_majflt_counter - pse->vmem_majflt_counter;
			}
			pse->vmem_majflt_counter = entry->vmem_majflt_counter;
		}

		ps->vmem_minflt_counter += pse->vmem_minflt;
		ps->vmem_majflt_counter += pse->vmem_majflt;

		if ((entry->cpu_user_counter == 0)
				&& (entry->cpu_system_counter == 0))
		{
			pse->cpu_user_counter += entry->cpu_user;
			pse->cpu_user = entry->cpu_user;

			pse->cpu_system_counter += entry->cpu_system;
			pse->cpu_system = entry->cpu_system;
		}
		else
		{
			if (entry->cpu_user_counter < pse->cpu_user_counter)
			{
				pse->cpu_user = entry->cpu_user_counter
					+ (ULONG_MAX - pse->cpu_user_counter);
			}
			else
			{
				pse->cpu_user = entry->cpu_user_counter - pse->cpu_user_counter;
			}
			pse->cpu_user_counter = entry->cpu_user_counter;

			if (entry->cpu_system_counter < pse->cpu_system_counter)
			{
				pse->cpu_system = entry->cpu_system_counter
					+ (ULONG_MAX - pse->cpu_system_counter);
			}
			else
			{
				pse->cpu_system = entry->cpu_system_counter - pse->cpu_system_counter;
			}
			pse->cpu_system_counter = entry->cpu_system_counter;
		}

		ps->cpu_user_counter   += pse->cpu_user;
		ps->cpu_system_counter += pse->cpu_system;
	}
}

/* remove old entries from instances of processes in list_head_g */
static void jobmetrics_list_reset (void)
{
	procstat_t *ps;
	procstat_entry_t *pse;
	procstat_entry_t *pse_prev;

	ps = NULL;
	pse = NULL;
	pse_prev = NULL;

	for (ps = list_head_g; ps != NULL; ps = ps->next)
	{
		ps->num_proc    = 0;
		ps->num_lwp     = 0;
		ps->vmem_size   = 0;
		ps->vmem_rss    = 0;
		ps->vmem_data   = 0;
		ps->vmem_code   = 0;
		ps->stack_size  = 0;
		ps->io_rchar = -1;
		ps->io_wchar = -1;
		ps->io_syscr = -1;
		ps->io_syscw = -1;

		pse_prev = NULL;
		pse = ps->instances;
		while (pse != NULL)
		{
			if (pse->age > 10)
			{
				DEBUG ("Removing this procstat entry cause it's too old: "
						"id = %lu; name = %s;",
						pse->id, ps->name);

				if (pse_prev == NULL)
				{
					ps->instances = pse->next;
					free (pse);
					pse = ps->instances;
				}
				else
				{
					pse_prev->next = pse->next;
					free (pse);
					pse = pse_prev->next;
				}
			}
			else
			{
				pse->age++;
				pse_prev = pse;
				pse = pse->next;
			}
		} 
	}
}

static int jobmetrics_init (void)
{
#if KERNEL_LINUX
	pagesize_g = sysconf(_SC_PAGESIZE);
	DEBUG ("pagesize_g = %li; CONFIG_HZ = %i;",
			pagesize_g, CONFIG_HZ);
/* #endif KERNEL_LINUX */
#endif
 INFO ("Jobmetrics plugin: init");
	return (0);
} /* int jobmetrics_init */

/* submit global state (e.g.: qty of zombies, running, etc..) */
static void jobmetrics_submit_state (const char *state, double value)
{
	value_t values[1];
	value_list_t vl = VALUE_LIST_INIT;

	values[0].gauge = value;

	vl.values = values;
	vl.values_len = 1;
	sstrncpy (vl.host, hostname_g, sizeof (vl.host));
	sstrncpy (vl.plugin, "jobmetrics", sizeof (vl.plugin));
	sstrncpy (vl.plugin_instance, "", sizeof (vl.plugin_instance));
	sstrncpy (vl.type, "jm_state", sizeof (vl.type));
	sstrncpy (vl.type_instance, state, sizeof (vl.type_instance));

	plugin_dispatch_values (&vl);
}

/* submit info about specific process (e.g.: memory taken, cpu usage, etc..) */
static void jobmetrics_submit_proc_list (procstat_t *ps)
{

	value_t values[2];
	value_list_t vl = VALUE_LIST_INIT;

	vl.values = values;
	vl.values_len = 2;
	sstrncpy (vl.host, hostname_g, sizeof (vl.host));
	sstrncpy (vl.plugin, "job", sizeof (vl.plugin));
	sstrncpy (vl.plugin_instance, ps->jobId, sizeof (vl.plugin_instance));

	sstrncpy (vl.type, "jm_vm", sizeof (vl.type));
	vl.values[0].gauge = ps->vmem_size;
	vl.values_len = 1;
	plugin_dispatch_values (&vl);

	sstrncpy (vl.type, "jm_rss", sizeof (vl.type));
	vl.values[0].gauge = ps->vmem_rss;
	vl.values_len = 1;
	plugin_dispatch_values (&vl);

	sstrncpy (vl.type, "jm_data", sizeof (vl.type));
	vl.values[0].gauge = ps->vmem_data;
	vl.values_len = 1;
	plugin_dispatch_values (&vl);

	sstrncpy (vl.type, "jm_code", sizeof (vl.type));
	vl.values[0].gauge = ps->vmem_code;
	vl.values_len = 1;
	plugin_dispatch_values (&vl);

	sstrncpy (vl.type, "jm_stacksize", sizeof (vl.type));
	vl.values[0].gauge = ps->stack_size;
	vl.values_len = 1;
	plugin_dispatch_values (&vl);

	sstrncpy (vl.type, "jm_cputime", sizeof (vl.type));
	vl.values[0].derive = ps->cpu_user_counter;
	vl.values[1].derive = ps->cpu_system_counter;
	vl.values_len = 2;
	plugin_dispatch_values (&vl);

	sstrncpy (vl.type, "jm_count", sizeof (vl.type));
	vl.values[0].gauge = ps->num_proc;
	vl.values[1].gauge = ps->num_lwp;
	vl.values_len = 2;
	plugin_dispatch_values (&vl);

	sstrncpy (vl.type, "jm_pagefaults", sizeof (vl.type));
	vl.values[0].derive = ps->vmem_minflt_counter;
	vl.values[1].derive = ps->vmem_majflt_counter;
	vl.values_len = 2;
	plugin_dispatch_values (&vl);

	if ( (ps->io_rchar != -1) && (ps->io_wchar != -1) )
	{
		sstrncpy (vl.type, "jm_disk_octets", sizeof (vl.type));
		vl.values[0].derive = ps->io_rchar;
		vl.values[1].derive = ps->io_wchar;
		vl.values_len = 2;
		plugin_dispatch_values (&vl);
	}

	if ( (ps->io_syscr != -1) && (ps->io_syscw != -1) )
	{
		sstrncpy (vl.type, "jm_disk_ops", sizeof (vl.type));
		vl.values[0].derive = ps->io_syscr;
		vl.values[1].derive = ps->io_syscw;
		vl.values_len = 2;
		plugin_dispatch_values (&vl);
	}


	DEBUG ("list_submit jobId = %s; name = %s; num_proc = %lu; num_lwp = %lu; "
			"vmem_size = %lu; vmem_rss = %lu; vmem_data = %lu; "
			"vmem_code = %lu; "
			"vmem_minflt_counter = %"PRIi64"; vmem_majflt_counter = %"PRIi64"; "
			"cpu_user_counter = %"PRIi64"; cpu_system_counter = %"PRIi64"; "
			"io_rchar = %"PRIi64"; io_wchar = %"PRIi64"; "
			"io_syscr = %"PRIi64"; io_syscw = %"PRIi64";",
			ps->jobId, ps->name, ps->num_proc, ps->num_lwp,
			ps->vmem_size, ps->vmem_rss,
			ps->vmem_data, ps->vmem_code,
			ps->vmem_minflt_counter, ps->vmem_majflt_counter,
			ps->cpu_user_counter, ps->cpu_system_counter,
			ps->io_rchar, ps->io_wchar, ps->io_syscr, ps->io_syscw);

} /* void jobmetrics_submit_proc_list */

/* submit info about specific process (e.g.: memory taken, cpu usage, etc..) instances*/
static void jobmetrics_submit_proc_sublist (procstat_t *psj)
{

    value_t values[2];
    value_list_t vl = VALUE_LIST_INIT;
    procstat_entry_t *ps;
    char instance[1024];

    for ( ps = psj->instances; ps != NULL; ps = ps->next)
    {
        vl.values = values;
        vl.values_len = 2;
        sstrncpy (vl.host, hostname_g, sizeof (vl.host));
        sstrncpy (vl.plugin, "jobProcess", sizeof (vl.plugin));
        sprintf (instance,"%s-%lu", psj->jobId, ps->id);
        sstrncpy (vl.plugin_instance, instance, sizeof (vl.plugin_instance));

        sstrncpy (vl.type, "jm_vm", sizeof (vl.type));
        vl.values[0].gauge = ps->vmem_size;
        vl.values_len = 1;
        plugin_dispatch_values (&vl);

        sstrncpy (vl.type, "jm_rss", sizeof (vl.type));
        vl.values[0].gauge = ps->vmem_rss;
        vl.values_len = 1;
        plugin_dispatch_values (&vl);

        sstrncpy (vl.type, "jm_data", sizeof (vl.type));
        vl.values[0].gauge = ps->vmem_data;
        vl.values_len = 1;
        plugin_dispatch_values (&vl);

        sstrncpy (vl.type, "jm_code", sizeof (vl.type));
        vl.values[0].gauge = ps->vmem_code;
        vl.values_len = 1;
        plugin_dispatch_values (&vl);

        sstrncpy (vl.type, "jm_stacksize", sizeof (vl.type));
        vl.values[0].gauge = ps->stack_size;
        vl.values_len = 1;
        plugin_dispatch_values (&vl);

        sstrncpy (vl.type, "jm_cputime", sizeof (vl.type));
        vl.values[0].derive = ps->cpu_user_counter;
        vl.values[1].derive = ps->cpu_system_counter;
        vl.values_len = 2;
        plugin_dispatch_values (&vl);

        sstrncpy (vl.type, "jm_count", sizeof (vl.type));
        vl.values[0].gauge = ps->num_proc;
        vl.values[1].gauge = ps->num_lwp;
        vl.values_len = 2;
        plugin_dispatch_values (&vl);

        sstrncpy (vl.type, "jm_pagefaults", sizeof (vl.type));
        vl.values[0].derive = ps->vmem_minflt_counter;
        vl.values[1].derive = ps->vmem_majflt_counter;
        vl.values_len = 2;
        plugin_dispatch_values (&vl);

        if ( (ps->io_rchar != -1) && (ps->io_wchar != -1) )
        {
            sstrncpy (vl.type, "jm_disk_octets", sizeof (vl.type));
            vl.values[0].derive = ps->io_rchar;
            vl.values[1].derive = ps->io_wchar;
            vl.values_len = 2;
            plugin_dispatch_values (&vl);
        }
        if ( (ps->io_syscr != -1) && (ps->io_syscw != -1) )
        {
            sstrncpy (vl.type, "jm_disk_ops", sizeof (vl.type));
            vl.values[0].derive = ps->io_syscr;
            vl.values[1].derive = ps->io_syscw;
            vl.values_len = 2;
            plugin_dispatch_values (&vl);
        }

        DEBUG ("list_submit jobId = %s; name = %s; num_proc = %lu; num_lwp = %lu; "
                "vmem_size = %lu; vmem_rss = %lu; vmem_data = %lu; "
                "vmem_code = %lu; "
                "vmem_minflt_counter = %"PRIi64"; vmem_majflt_counter = %"PRIi64"; "
                "cpu_user_counter = %"PRIi64"; cpu_system_counter = %"PRIi64"; "
                "io_rchar = %"PRIi64"; io_wchar = %"PRIi64"; "
                "io_syscr = %"PRIi64"; io_syscw = %"PRIi64";",
                psj->jobId, psj->name, ps->num_proc, ps->num_lwp,
                ps->vmem_size, ps->vmem_rss,
                ps->vmem_data, ps->vmem_code,
                ps->vmem_minflt_counter, ps->vmem_majflt_counter,
                ps->cpu_user_counter, ps->cpu_system_counter,
                ps->io_rchar, ps->io_wchar, ps->io_syscr, ps->io_syscw);
    }
}

#if KERNEL_LINUX 
static void jobmetrics_submit_fork_rate (derive_t value)
{
	value_t values[1];
	value_list_t vl = VALUE_LIST_INIT;

	values[0].derive = value;

	vl.values = values;
	vl.values_len = 1;
	sstrncpy(vl.host, hostname_g, sizeof (vl.host));
	sstrncpy(vl.plugin, "jobmetrics", sizeof (vl.plugin));
	sstrncpy(vl.plugin_instance, "", sizeof (vl.plugin_instance));
	sstrncpy(vl.type, "jm_fork_rate", sizeof (vl.type));
	sstrncpy(vl.type_instance, "", sizeof (vl.type_instance));

	plugin_dispatch_values(&vl);
}
#endif /* KERNEL_LINUX*/

/* ------- additional functions for KERNEL_LINUX/HAVE_THREAD_INFO ------- */
#if KERNEL_LINUX
static int jobmetrics_read_tasks (int pid)
{
	char           dirname[64];
	DIR           *dh;
	struct dirent *ent;
	int count = 0;

	ssnprintf (dirname, sizeof (dirname), "/proc/%i/task", pid);

	if ((dh = opendir (dirname)) == NULL)
	{
		DEBUG ("Failed to open directory `%s'", dirname);
		return (-1);
	}

	while ((ent = readdir (dh)) != NULL)
	{
		if (!isdigit ((int) ent->d_name[0]))
			continue;
		else
			count++;
	}
	closedir (dh);

	return ((count >= 1) ? count : 1);
} /* int *jobmetrics_read_tasks */

/* Read advanced virtual memory data from /proc/pid/status */
static procstat_t *jobmetrics_read_vmem (int pid, procstat_t *ps)
{
	FILE *fh;
	char buffer[1024];
	char filename[64];
	unsigned long long lib = 0;
	unsigned long long exe = 0;
	unsigned long long data = 0;
	char *fields[8];
	int numfields;

	ssnprintf (filename, sizeof (filename), "/proc/%i/status", pid);
	if ((fh = fopen (filename, "r")) == NULL)
		return (NULL);

	while (fgets (buffer, sizeof(buffer), fh) != NULL)
	{
		long long tmp;
		char *endptr;

		if (strncmp (buffer, "Vm", 2) != 0)
			continue;

		numfields = strsplit (buffer, fields,
				STATIC_ARRAY_SIZE (fields));

		if (numfields < 2)
			continue;

		errno = 0;
		endptr = NULL;
		tmp = strtoll (fields[1], &endptr, /* base = */ 10);
		if ((errno == 0) && (endptr != fields[1]))
		{
			if (strncmp (buffer, "VmData", 6) == 0)
			{
				data = tmp;
			}
			else if (strncmp (buffer, "VmLib", 5) == 0)
			{
				lib = tmp;
			}
			else if  (strncmp(buffer, "VmExe", 5) == 0)
			{
				exe = tmp;
			}
		}
	} /* while (fgets) */

	if (fclose (fh))
	{
		char errbuf[1024];
		WARNING ("processes: fclose: %s",
				sstrerror (errno, errbuf, sizeof (errbuf)));
	}

	ps->vmem_data = data * 1024;
	ps->vmem_code = (exe + lib) * 1024;

	return (ps);
} /* procstat_t *jobmetrics_read_vmem */

static procstat_t *jobmetrics_read_io (int pid, procstat_t *ps)
{
	FILE *fh;
	char buffer[1024];
	char filename[64];

	char *fields[8];
	int numfields;

	ssnprintf (filename, sizeof (filename), "/proc/%i/io", pid);
	if ((fh = fopen (filename, "r")) == NULL)
		return (NULL);

	while (fgets (buffer, sizeof (buffer), fh) != NULL)
	{
		derive_t *val = NULL;
		long long tmp;
		char *endptr;

		if (strncasecmp (buffer, "rchar:", 6) == 0)
			val = &(ps->io_rchar);
		else if (strncasecmp (buffer, "wchar:", 6) == 0)
			val = &(ps->io_wchar);
		else if (strncasecmp (buffer, "syscr:", 6) == 0)
			val = &(ps->io_syscr);
		else if (strncasecmp (buffer, "syscw:", 6) == 0)
			val = &(ps->io_syscw);
		else
			continue;

		numfields = strsplit (buffer, fields,
				STATIC_ARRAY_SIZE (fields));

		if (numfields < 2)
			continue;

		errno = 0;
		endptr = NULL;
		tmp = strtoll (fields[1], &endptr, /* base = */ 10);
		if ((errno != 0) || (endptr == fields[1]))
			*val = -1;
		else
			*val = (derive_t) tmp;
	} /* while (fgets) */

	if (fclose (fh))
	{
		char errbuf[1024];
		WARNING ("processes: fclose: %s",
				sstrerror (errno, errbuf, sizeof (errbuf)));
	}

	return (ps);
} /* procstat_t *ps_read_io */

static procstat_t *jobmetrics_read_ctxt (int pid, procstat_t *ps)
{
    FILE *fh;
    char  filename[64];
    char  buffer[1024];

    char *fields[64];
    int  numfields;

    ssnprintf (filename, sizeof (filename), "/proc/%i/status", pid);
    if ((fh = fopen (filename, "r")) == NULL)
                return NULL;
    while (fgets (buffer, sizeof(buffer), fh) != NULL)
    {

       if (strncmp (buffer, "voluntary_ctxt_switches", 23) == 0){
          numfields = strsplit (buffer, fields,STATIC_ARRAY_SIZE (fields));
          ps->voluntary_ctxt_switches = atoll(fields[1]);
       }
       if (strncmp (buffer, "nonvoluntary_ctxt_switches", 26) == 0){
          numfields = strsplit (buffer, fields,STATIC_ARRAY_SIZE (fields));
          ps->nonvoluntary_ctxt_switches = atoll(fields[1]);
       }
    }
    fclose (fh);

    return ps;
}

int jobmetrics_read_process (int pid, procstat_t *ps, char *state)
{
	char  filename[64];
	char  buffer[1024];

	char *fields[64];
	char  fields_len;

	int   buffer_len;

	char *buffer_ptr;
	size_t name_start_pos;
	size_t name_end_pos;
	size_t name_len;

	derive_t cpu_user_counter;
	derive_t cpu_system_counter;
	long long unsigned vmem_size;
	long long unsigned vmem_rss;
	long long unsigned stack_size;

	memset (ps, 0, sizeof (procstat_t));

	ssnprintf (filename, sizeof (filename), "/proc/%i/stat", pid);

	buffer_len = read_file_contents (filename,
			buffer, sizeof(buffer) - 1);
	if (buffer_len <= 0)
		return (-1);
	buffer[buffer_len] = 0;

	/* The name of the process is enclosed in parens. Since the name can
	 * contain parens itself, spaces, numbers and pretty much everything
	 * else, use these to determine the process name. We don't use
	 * strchr(3) and strrchr(3) to avoid pointer arithmetic which would
	 * otherwise be required to determine name_len. */
	name_start_pos = 0;
	while ((buffer[name_start_pos] != '(')
			&& (name_start_pos < buffer_len))
		name_start_pos++;

	name_end_pos = buffer_len;
	while ((buffer[name_end_pos] != ')')
			&& (name_end_pos > 0))
		name_end_pos--;

	/* Either '(' or ')' is not found or they are in the wrong order.
	 * Anyway, something weird that shouldn't happen ever. */
	if (name_start_pos >= name_end_pos)
	{
		ERROR ("jobmetrics plugin: name_start_pos = %zu >= name_end_pos = %zu",
				name_start_pos, name_end_pos);
		return (-1);
	}

	name_len = (name_end_pos - name_start_pos) - 1;
	if (name_len >= sizeof (ps->name))
		name_len = sizeof (ps->name) - 1;

	sstrncpy (ps->name, &buffer[name_start_pos + 1], name_len + 1);

	if ((buffer_len - name_end_pos) < 2)
		return (-1);
	buffer_ptr = &buffer[name_end_pos + 2];

	fields_len = strsplit (buffer_ptr, fields, STATIC_ARRAY_SIZE (fields));
	if (fields_len < 22)
	{
		DEBUG ("jobmetrics plugin: jobmetrics_read_process (pid = %i):"
				" `%s' has only %i fields..",
				(int) pid, filename, fields_len);
		return (-1);
	}

	*state = fields[0][0];

	if (*state == 'Z')
	{
		ps->num_lwp  = 0;
		ps->num_proc = 0;
	}
	else
	{
		if ( (ps->num_lwp = jobmetrics_read_tasks (pid)) == -1 )
		{
			/* returns -1 => kernel 2.4 */
			ps->num_lwp = 1;
		}
		ps->num_proc = 1;
	}

	/* Leave the rest at zero if this is only a zombi */
	if (ps->num_proc == 0)
	{
		DEBUG ("jobmetrics plugin: This is only a zombi: pid = %i; "
				"name = %s;", pid, ps->name);
		return (0);
	}

	cpu_user_counter   = atoll (fields[11]);
	cpu_system_counter = atoll (fields[12]);
	vmem_size          = atoll (fields[20]);
	vmem_rss           = atoll (fields[21]);
	ps->vmem_minflt_counter = atol (fields[7]);
	ps->vmem_majflt_counter = atol (fields[9]);

	{
		unsigned long long stack_start = atoll (fields[25]);
		unsigned long long stack_ptr   = atoll (fields[26]);

		stack_size = (stack_start > stack_ptr)
			? stack_start - stack_ptr
			: stack_ptr - stack_start;
	}

	/* Convert jiffies to useconds */
	cpu_user_counter   = cpu_user_counter   * 1000000 / CONFIG_HZ;
	cpu_system_counter = cpu_system_counter * 1000000 / CONFIG_HZ;
	vmem_rss = vmem_rss * pagesize_g;

	if ( (jobmetrics_read_vmem(pid, ps)) == NULL)
	{
		/* No VMem data */
		ps->vmem_data = -1;
		ps->vmem_code = -1;
		DEBUG("jobmetrics_read_process: did not get vmem data for pid %i",pid);
	}

	ps->cpu_user_counter = cpu_user_counter;
	ps->cpu_system_counter = cpu_system_counter;
	ps->vmem_size = (unsigned long) vmem_size;
	ps->vmem_rss = (unsigned long) vmem_rss;
	ps->stack_size = (unsigned long) stack_size;

	if ( (jobmetrics_read_io (pid, ps)) == NULL)
	{
		/* no io data */
		ps->io_rchar = -1;
		ps->io_wchar = -1;
		ps->io_syscr = -1;
		ps->io_syscw = -1;

		DEBUG("jobmetrics_read_process: not get io data for pid %i",pid);
	}

    if ( ( jobmetrics_read_ctxt(pid, ps)) == NULL)
    {
        ps->voluntary_ctxt_switches = -1;
        ps->nonvoluntary_ctxt_switches = -1;

        DEBUG("jobmetrics_read_process: not get ctxt data for pid %i",pid);
    }

	DEBUG ("jobmetrics_read_process; name = %s; num_proc = %lu; num_lwp = %lu; "
                        "vmem_size = %lu; vmem_rss = %lu; vmem_data = %lu; "
                        "vmem_code = %lu; "
                        "vmem_minflt_counter = %"PRIi64"; vmem_majflt_counter = %"PRIi64"; "
                        "cpu_user_counter = %"PRIi64"; cpu_system_counter = %"PRIi64"; "
                        "io_rchar = %"PRIi64"; io_wchar = %"PRIi64"; "
                        "io_syscr = %"PRIi64"; io_syscw = %"PRIi64";",
                         ps->name, ps->num_proc, ps->num_lwp,
                        ps->vmem_size, ps->vmem_rss,
                        ps->vmem_data, ps->vmem_code,
                        ps->vmem_minflt_counter, ps->vmem_majflt_counter,
                        ps->cpu_user_counter, ps->cpu_system_counter,
                        ps->io_rchar, ps->io_wchar, ps->io_syscr, ps->io_syscw);

	/* success */
	return (0);
} /* int ps_read_process (...) */

static char *jobmetrics_get_cmdline (pid_t pid, char *name, char *buf, size_t buf_len)
{
	char  *buf_ptr;
	size_t len;

	char file[PATH_MAX];
	int  fd;

	size_t n;

	if ((pid < 1) || (NULL == buf) || (buf_len < 2))
		return NULL;

	ssnprintf (file, sizeof (file), "/proc/%u/cmdline",
		       	(unsigned int) pid);

	errno = 0;
	fd = open (file, O_RDONLY);
	if (fd < 0) {
		char errbuf[4096];
		/* ENOENT means the process exited while we were handling it.
		 * Don't complain about this, it only fills the logs. */
		if (errno != ENOENT)
			WARNING ("jobmetrics plugin: Failed to open `%s': %s.", file,
					sstrerror (errno, errbuf, sizeof (errbuf)));
		return NULL;
	}

	buf_ptr = buf;
	len     = buf_len;

	n = 0;

	while (42) {
		ssize_t status;

		status = read (fd, (void *)buf_ptr, len);

		if (status < 0) {
			char errbuf[1024];

			if ((EAGAIN == errno) || (EINTR == errno))
				continue;

			WARNING ("jobmetrics plugin: Failed to read from `%s': %s.", file,
					sstrerror (errno, errbuf, sizeof (errbuf)));
			close (fd);
			return NULL;
		}

		n += status;

		if (status == 0)
			break;

		buf_ptr += status;
		len     -= status;

		if (len <= 0)
			break;
	}

	close (fd);

	if (0 == n) {
		/* cmdline not available; e.g. kernel thread, zombie */
		if (NULL == name)
			return NULL;

		ssnprintf (buf, buf_len, "[%s]", name);
		return buf;
	}

	assert (n <= buf_len);

	if (n == buf_len)
		--n;
	buf[n] = '\0';

	--n;
	/* remove trailing whitespace */
	while ((n > 0) && (isspace (buf[n]) || ('\0' == buf[n]))) {
		buf[n] = '\0';
		--n;
	}

	/* arguments are separated by '\0' in /proc/<pid>/cmdline */
	while (n > 0) {
		if ('\0' == buf[n])
			buf[n] = ' ';
		--n;
	}
	return buf;
} /* char *ps_get_cmdline (...) */

static int read_fork_rate ()
{
	FILE *proc_stat;
	char buffer[1024];
	value_t value;
	_Bool value_valid = 0;

	proc_stat = fopen ("/proc/stat", "r");
	if (proc_stat == NULL)
	{
		char errbuf[1024];
		ERROR ("jobmetrics plugin: fopen (/proc/stat) failed: %s",
				sstrerror (errno, errbuf, sizeof (errbuf)));
		return (-1);
	}

	while (fgets (buffer, sizeof (buffer), proc_stat) != NULL)
	{
		int status;
		char *fields[3];
		int fields_num;

		fields_num = strsplit (buffer, fields,
				STATIC_ARRAY_SIZE (fields));
		if (fields_num != 2)
			continue;

		if (strcmp ("processes", fields[0]) != 0)
			continue;

		status = parse_value (fields[1], &value, DS_TYPE_DERIVE);
		if (status == 0)
			value_valid = 1;

		break;
	}
	fclose(proc_stat);

	if (!value_valid)
		return (-1);

	jobmetrics_submit_fork_rate (value.derive);
	return (0);
}
#endif /*KERNEL_LINUX */

static void jobmetrics_read_name(char *fl_name, char *name)
{
    int ch, i;

    for (ch = 0; fl_name[ch] != '\0'; ch++)
    {
       i = 0;
       if ( fl_name[ch] == ' ')
       {
          ch = ch + 2;
          while (fl_name[ch] != ' '){
             name[i] = fl_name[ch];
             i++;
             ch++;
          }
          name[--i] = '\0';
          break;
       }
    }
}

static void jobmetrics_read_jobid(char *dir_name, char *jobId)
{
    int ch, i;

    for ( ch = 0; dir_name[ch] != '\0'; ch++)
    {
        if ( dir_name[ch] == '.' ){
            ch++;
            i = 0;
            while (dir_name[ch] != '.' && dir_name[ch] != '[')
            {
               jobId[i] = dir_name[ch];
               i++;
               ch++;
            }
            jobId[i] = '\0';
            break;
        }
    }
}

static int isthread(int pid)
{
    FILE *fh;
    char  filename[64];
    char  buffer[1024];

    char *fields[64];
    int  numfields;

    ssnprintf (filename, sizeof (filename), "/proc/%i/status", pid);
    if ((fh = fopen (filename, "r")) == NULL)
                return (0);
    while (fgets (buffer, sizeof(buffer), fh) != NULL)
    {

       if (strncmp (buffer, "Tgid", 4) != 0)
                   continue;
        
       numfields = strsplit (buffer, fields,STATIC_ARRAY_SIZE (fields)); 
       break; 
    }
    fclose (fh);

    if (atoi(fields[1]) == pid)
        return 0;
    else
        return 1;

}

/* do actual readings from kernel */
static int jobmetrics_read (void)
{

#if KERNEL_LINUX
	int running  = 0;
	int sleeping = 0;
	int zombies  = 0;
	int stopped  = 0;
	int paging   = 0;
	int blocked  = 0;

    struct dirent *ent;
	DIR           *proc;
	int            pid;

	char cmdline[CMDLINE_BUFFER_SIZE];

	int        status;
	procstat_t ps;
	procstat_entry_t pse;
	char       state;

    char        jobId[PROCSTAT_NAME_LEN], name[PROCSTAT_NAME_LEN];
	char        filename[100], line[80];
	FILE        *fp,*fh;
	procstat_t  *ps_ptr;
	
	running = sleeping = zombies = stopped = paging = blocked = 0;
	jobmetrics_list_reset ();

	if ((proc = opendir ("/cgroup/cpuset/lsf/euler")) == NULL)
	{
		char errbuf[1024];
		ERROR ("Cannot open `/cgroup/cpuset/lsf/euler': %s",
				sstrerror (errno, errbuf, sizeof (errbuf)));
		return (-1);
	}

	while ((ent = readdir (proc)) != NULL)
	{
	   if (ent->d_type & DT_DIR)
        {	
	     if (strcmp (ent->d_name, "..") != 0 && strcmp (ent->d_name, ".") != 0)
         {
            //get jobid
		    jobmetrics_read_jobid(ent->d_name, jobId);
		    sprintf( filename , "%s/%s/%s", "/cgroup/cpuset/lsf/euler", ent->d_name,"tasks");	
		    fp = fopen(filename,"r");
            if (fp == NULL ){
                     ERROR("jobmetrics plugin: could not open LSF fs");
                     return -1;
            }

            //if job is DONE
            size_t size;
            size = ftell(fp);
            fseek(fp, 0, SEEK_END);
            size = ftell(fp);
            if ( size > 0) 
                jobmetrics_list_remove (jobId);

            //read PIDs from a job	
		    while(fgets(line, 80, fp) != NULL)
             {
                sscanf (line, "%d", &pid);
           		sprintf (filename, "/proc/%i/stat", pid);
                if ((fh = fopen (filename, "r")) == NULL)	
                        	return -1;
                //get process name
			    name[0] = '\0';
                if (fgets(line, sizeof(line), fh) != NULL)
                	jobmetrics_read_name(line, name);
                fclose(fh);

                //if pid is not a thread than get some data from it
                if ((strcmp(name,"res") != 0) && (!isdigit(name[0]))
                        && (name[0] != '\0'))
                {
                    //we exclude any LSF process
     			    if ( !isthread(pid)) 
			        {

				        status = jobmetrics_read_process (pid, &ps, &state);
                        if (status != 0 )
				        {
					        ERROR ("jobmetrics_read_process failed: %i", status);
					        continue;
				        }
        
				        sstrncpy (ps.jobId, jobId, sizeof(ps.jobId));
                        sstrncpy (ps.name, name, sizeof(ps.name));

				        pse.id       = pid;
				        pse.age      = 0;

				        pse.num_proc   = ps.num_proc;
				        pse.num_lwp    = ps.num_lwp;
				        pse.vmem_size  = ps.vmem_size;
				        pse.vmem_rss   = ps.vmem_rss;
				        pse.vmem_data  = ps.vmem_data;
				        pse.vmem_code  = ps.vmem_code;
				        pse.stack_size = ps.stack_size;

				        pse.vmem_minflt = 0;
				        pse.vmem_minflt_counter = ps.vmem_minflt_counter;
				        pse.vmem_majflt = 0;
				        pse.vmem_majflt_counter = ps.vmem_majflt_counter;

				        pse.cpu_user = 0;
				        pse.cpu_user_counter = ps.cpu_user_counter;
				        pse.cpu_system = 0;
				        pse.cpu_system_counter = ps.cpu_system_counter;

				        pse.io_rchar = ps.io_rchar;
				        pse.io_wchar = ps.io_wchar;
				        pse.io_syscr = ps.io_syscr;
				        pse.io_syscw = ps.io_syscw;

				        switch (state)
				        {
					        case 'R': running++;  break;
					        case 'S': sleeping++; break;
					        case 'D': blocked++;  break;
					        case 'Z': zombies++;  break;
					        case 'T': stopped++;  break;
					        case 'W': paging++;   break;
				        }

				        jobmetrics_list_add (ps.jobId, ps.name, 
					        jobmetrics_get_cmdline (pid, ps.name, 
					        cmdline, sizeof (cmdline)),&pse);
			        }
		        }
	         }	
		        fclose(fp);
          }   
	    }
	}

	closedir (proc);

	jobmetrics_submit_state ("running",  running);
	jobmetrics_submit_state ("sleeping", sleeping);
	jobmetrics_submit_state ("zombies",  zombies);
	jobmetrics_submit_state ("stopped",  stopped);
	jobmetrics_submit_state ("paging",   paging);
	jobmetrics_submit_state ("blocked",  blocked);

	for (ps_ptr = list_head_g; ps_ptr != NULL; ps_ptr = ps_ptr->next)
	{
		jobmetrics_submit_proc_list (ps_ptr);
        jobmetrics_submit_proc_sublist (ps_ptr);
	}

	read_fork_rate();
/* #endif KERNEL_LINUX */
#endif 

	return (0);
} /* int jobmetrics_read */

void module_register (void)
{
	plugin_register_init ("jobmetrics", jobmetrics_init);
	plugin_register_read ("jobmetrics", jobmetrics_read);
} /* void module_register */
