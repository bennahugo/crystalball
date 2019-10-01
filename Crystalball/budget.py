import psutil
import numpy as np
import sys

from Crystalball import log

def get_budget(nr_sources,nr_rows,nr_chans,nr_corrs,data_type,cb_args,fudge_factor=2.,row2source_ratio=100):
    systmem=np.float(psutil.virtual_memory()[0])
    if not cb_args.num_workers: nrthreads=psutil.cpu_count()
    else: nrthreads=cb_args.num_workers

    log.info('-------------------------------------------')
    log.info('system RAM = {0:.2f} GB'.format(systmem/1024**3))
    log.info('nr of logical CPUs = {0:d}'.format(nrthreads))
    log.info('nr sources = {0:d}'.format(nr_sources))
    log.info('nr rows    = {0:d}'.format(nr_rows))
    log.info('nr chans   = {0:d}'.format(nr_chans))
    log.info('nr corrs   = {0:d}'.format(nr_corrs))

    data_type = {'complex':'complex64','dcomplex':'complex128'}[data_type]
    data_bytes = np.dtype(data_type).itemsize
    bytes_per_row_source = nr_chans*nr_corrs*data_bytes
    memory_per_row_source = bytes_per_row_source * fudge_factor

    if cb_args.model_chunks and cb_args.row_chunks:
        rows_per_chunk=cb_args.row_chunks
        sources_per_chunk=cb_args.model_chunks
        log.info('sources per chunk = {0:.0f} (user setting)'.format(sources_per_chunk))
        log.info('rows per chunk    = {0:.0f} (user setting)'.format(rows_per_chunk))
        memory_usage = rows_per_chunk*sources_per_chunk*memory_per_row_source*nrthreads
        log.info('expected memory usage = {0:.2f} GB'.format(memory_usage/1024**3))
    elif not cb_args.model_chunks and not cb_args.row_chunks:
        allowed_rowXsource_per_thread = systmem * cb_args.memory_fraction / memory_per_row_source / nrthreads
        rows_per_chunk    = np.int(np.minimum(nr_rows,np.sqrt(allowed_rowXsource_per_thread * row2source_ratio)))
        sources_per_chunk = np.int(np.minimum(nr_sources,rows_per_chunk / row2source_ratio))
        log.info('sources per chunk = {0:.0f} (auto setting)'.format(sources_per_chunk))
        log.info('rows per chunk    = {0:.0f} (auto setting)'.format(rows_per_chunk))
        memory_usage = rows_per_chunk*sources_per_chunk*memory_per_row_source*nrthreads
        log.info('expected memory usage = {0:.2f} GB'.format(memory_usage/1024**3))
    else:
        log.info('For now you must set both row and source chunk, or leave both unset (=0); you cannot set only one of them.')
        sys.exit(1)
    return rows_per_chunk,sources_per_chunk
