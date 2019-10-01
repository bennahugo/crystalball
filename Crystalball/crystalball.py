#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse

from dask.diagnostics import ProgressBar
import numpy as np
import sys


try:
    import dask
    import dask.array as da
    import xarray as xr
    from xarrayms import xds_from_ms, xds_from_table, xds_to_table
except ImportError as e:
    opt_import_error = e
else:
    opt_import_error = None

from scipy.optimize import curve_fit

from africanus.coordinates.dask import radec_to_lm
from africanus.rime.dask import phase_delay, predict_vis
from africanus.model.coherency.dask import convert
from africanus.model.shape.dask import gaussian
from africanus.util.requirements import requires_optional

import casacore.tables

from Crystalball.budget import get_budget
from Crystalball.ms import ms_preprocess
from Crystalball.wsclean import import_from_wsclean

from Crystalball import log

def create_parser():
    p = argparse.ArgumentParser()
    p.add_argument("ms",
                   help="Input .MS file.")
    p.add_argument("-sm", "--sky-model", default="sky-model.txt",
                   help="Name of file containing the sky model. "
                        "Default is 'sky-model.txt'")
    p.add_argument("-o", "--output-column", default="MODEL_DATA",
                   help="Output visibility column. Default is '%(default)s'")
    p.add_argument("-rc", "--row-chunks", type=int, default=0,
                   help="Number of rows of input .MS that are processed in a single chunk. "
                        "If 0 it will be set automatically. Default is 0.")
    p.add_argument("-mc", "--model-chunks", type=int, default=0,
                   help="Number of sky model components that are processed in a single chunk. "
                        "If 0 it wil be set automatically. Default is 0.")
    p.add_argument("--exp-sign-convention", choices=['casa', 'thompson'],
                   default='casa',
                   help="Sign convention to use for the complex exponential. "
                        "'casa' specifies the e^(2.pi.I) convention while "
                        "'thompson' specifies the e^(-2.pi.I) convention in "
                        "the white book and Fourier analysis literature. "
                        "Defaults to '%(default)s'")
    p.add_argument("-sp", "--spectra", action="store_true",
                   help="Optional. Model sources as non-flat spectra. The spectral "
                        "coefficients and reference frequency must be present in the sky model.")
    p.add_argument("-w", "--within", type=str,
                   help="Optional. Give JS9 region file. Only sources within those regions will be "
                        "included.")
    p.add_argument("-po", "--points-only", action="store_true",
                   help="Select only point-type sources.")
    p.add_argument("-ns", "--num-sources", type=int, default=0, metavar="N",
                   help="Select only N brightest sources.")
    p.add_argument("-j", "--num-workers", type=int, default=0, metavar="N",
                   help="Explicitly set the number of worker threads.")
    p.add_argument("-mf", "--memory-fraction", type=float, default=0.5,
                   help="Fraction of system RAM that can be used. Used when setting automatically the "
                        "chunk size. Default in 0.5.")
    p.add_argument("-f", "--fieldid", type=int, default=0,
                   help="Field to select for prediction")
    p.add_argument("-dc", "--dontcluster", action="store_true",
                   help="Do not cluster clean components and refit to order 4 polynomial")
    return p


def support_tables(args, tables):
    """
    Parameters
    ----------
    args : object
        Script argument objects
    tables : list of str
        List of support tables to open

    Returns
    -------
    table_map : dict of :class:`xarray.Dataset`
        {name: dataset}
    """
    return {t: [ds.compute() for ds in
                xds_from_table("::".join((args.ms, t)),
                               group_cols="__row__")]
            for t in tables}


def corr_schema(pol):
    """
    Parameters
    ----------
    pol : :class:`xarray.Dataset`

    Returns
    -------
    corr_schema : list of list
        correlation schema from the POLARIZATION table,
        `[[9, 10], [11, 12]]` for example
    """

    corrs = pol.NUM_CORR.values
    corr_types = pol.CORR_TYPE.values

    if corrs == 4:
        return [[corr_types[0], corr_types[1]],
                [corr_types[2], corr_types[3]]]  # (2, 2) shape
    elif corrs == 2:
        return [corr_types[0], corr_types[1]]    # (2, ) shape
    elif corrs == 1:
        return [corr_types[0]]                   # (1, ) shape
    else:
        raise ValueError("corrs %d not in (1, 2, 4)" % corrs)


def einsum_schema(pol, dospec):
    """
    Returns an einsum schema suitable for multiplying per-baseline
    phase and brightness terms.

    Parameters
    ----------
    pol : :class:`xarray.Dataset`

    Returns
    -------
    einsum_schema : str
    """
    corrs = pol.NUM_CORR.values

    if corrs == 4:
        if dospec:
            return "srf, sfij -> srfij"
        else:
            return "srf, sij -> srfij"
    elif corrs in (2, 1):
        if dospec:
            return "srf, sfi -> srfi"
        else:
            return "srf, si -> srfi"
    else:
        raise ValueError("corrs %d not in (1, 2, 4)" % corrs)


@requires_optional("dask.array", "xarray", "xarrayms", opt_import_error)
def predict(args):
    # get inclusion regions
    include_regions = []
    exclude_regions = []
    if args.within:
        from regions import read_ds9
        import tempfile
        # kludge because regions cries over "FK5", wants lowercase
        with tempfile.NamedTemporaryFile(mode = "w") as tmpfile, open(args.within) as regfile:
            tmpfile.write(regfile.read().lower())
            tmpfile.flush()
            include_regions = read_ds9(tmpfile.name)
            log.info("read {} inclusion region(s) from {}".format(len(include_regions), args.within))

    # Import source data from WSClean component list
    # See https://sourceforge.net/p/wsclean/wiki/ComponentList
    (comp_type, radec, stokes,
     spec_coeff, ref_freq, log_spec_ind,
     gaussian_shape) = import_from_wsclean(args.sky_model,
                                           include_regions=include_regions,
                                           exclude_regions=exclude_regions,
                                           point_only=args.points_only,
                                           num=args.num_sources or None)

    # Get the support tables
    tables = support_tables(args, ["FIELD", "DATA_DESCRIPTION",
                                   "SPECTRAL_WINDOW", "POLARIZATION"])

    field_ds = tables["FIELD"]
    ddid_ds = tables["DATA_DESCRIPTION"]
    spw_ds = tables["SPECTRAL_WINDOW"]
    pol_ds = tables["POLARIZATION"]
    frequencies = np.sort([spw_ds[dd].CHAN_FREQ.data.flatten().values 
                            for dd in range(len(spw_ds))])
                            
    
    # cluster sources and refit. This only works for delta scale sources
    def __cluster(comp_type, radec, stokes, spec_coeff, ref_freq, log_spec_ind,
                  gaussian_shape, frequencies):
        uniq_radec = np.unique(radec)
        ncomp_type = []
        nradec = []
        nstokes = []
        nspec_coef = []
        nref_freq = []
        nlog_spec_ind = []
        ngaussian_shape = []

        for urd in uniq_radec:
            deltasel = comp_type[urd] == "POINT"
            polyspecsel = np.logical_not(spec_coef[urd])
            sel = deltasel & polyspecsel
            Is=stokes[sel, 0, None] * frequency[None,:]**0
            for jj in range(spec_coeff.shape[1]):                
                Is+=spec_coeff[sel, jj, None]*(frequency[None, :]/ref_freq[sel, None]-1)**(jj+1)
            Is = np.sum(Is, axis=0) # collapse over all the sources at this position
            logpolyspecsel = np.logical_not(log_spec_coef[urd])
            sel = deltasel & logpolyspecsel
            
            Is=np.log(stokes[sel, 0, None] * frequency[None,:]**0)
            for jj in range(spec_coeff.shape[1]):                
                Is+=spec_coeff[sel,jj,None]*da.log((frequency[None,:]/ref_freq[sel,None])**(jj+1))
            Is = np.exp(Is)
            Islogpoly = np.sum(Is, axis=0) # collapse over all the sources at this position

            popt, pfitvar = curve_fit(lambda i, a, b, c, d: i + a * (frequency/ref_freq[0, None] - 1) + 
                                      b * (frequency/ref_freq[0, None] - 1)**2 + c * (frequency/ref_freq[sel, None] - 1)**3 + 
                                      d * (frequency/ref_freq[0, None] - 1)**3, frequency, Ispoly + Islogpoly)
            if not np.all(np.isfinite(pfitvar)):
                popt[0] = np.sum(stokes[sel, 0, None], axis=0)
                popt[1:] = np.inf
                log.warn("Refitting at position {0:s} failed. Assuming flat spectrum source of {1:.2f} Jy".format(radec, popt[0]))
            else: 
                pcov = np.sqrt(np.diag(pfitvar))
                log.info("New fitted flux {0:.3f} Jy at position {1:s} with covariance {2:s}".format(
                    popt[0], radec, ", ".join([str(poptp) for poptp in popt])))

            ncomp_type.append("POINT")
            nradec.append(urd)
            nstokes.append(popt[0])
            nspec_coef.append(popt[1:])
            nref_freq.append(ref_freq[0])
            nlog_spec_ind = 0.0
            
        # add back all the gaussians
        sel = comp_type[radec] == "GAUSSIAN"
        for rd, stks, spec, ref, lspec, gs in zip(radec[sel], 
                                                  stokes[sel], 
                                                  spec_coef[sel], 
                                                  ref_freq[sel], 
                                                  log_spec_ind[sel],
                                                  gaussian_shape[sel]):
            ncomp_type.append("GAUSSIAN")
            nradec.append(rd)
            nstokes.append(stks)
            nspec_coef.append(spec)
            nref_freq.append(ref)
            nlog_spec_ind.append(lspec)
            ngaussian_shape.append(gs)

        log.info("Reduced {0:d} components to {1:d} components through by refitting".format(len(comp_type), len(ncomp_type)))
        return (np.array(ncomp_type), np.array(nradec), np.array(nstokes), 
                np.array(nspec_coeff), np.array(nref_freq), np.array(nlog_spec_ind),
                np.array(ngaussian_shape))

    if not args.dontcluster:
        (comp_type, radec, stokes,
        spec_coeff, ref_freq, log_spec_ind,
        gaussian_shape) = __cluster(comp_type, 
                                    radec, 
                                    stokes,
                                    spec_coeff, 
                                    ref_freq, 
                                    log_spec_ind,
                                    gaussian_shape,
                                    frequencies)

    # Add output column if it isn't present
    ms_rows,ms_datatype = ms_preprocess(args)

    # sort out resources
    args.row_chunks,args.model_chunks = get_budget(comp_type.shape[0],ms_rows,max([ss.NUM_CHAN.data for ss in spw_ds]),max([ss.NUM_CORR.data for ss in pol_ds]),ms_datatype,args)

    radec = da.from_array(radec, chunks=(args.model_chunks, 2))
    stokes = da.from_array(stokes, chunks=(args.model_chunks, 4))

    if np.count_nonzero(comp_type == 'GAUSSIAN') > 0:
        gaussian_components = True
        gshape_chunks = (args.model_chunks, 3)
        gaussian_shape = da.from_array(gaussian_shape, chunks=gshape_chunks)
    else:
        gaussian_components = False

    if args.spectra:
        spec_chunks = (args.model_chunks, spec_coeff.shape[1])
        spec_coeff = da.from_array(spec_coeff, chunks=spec_chunks)
        ref_freq = da.from_array(ref_freq, chunks=(args.model_chunks,))

    # List of write operations
    writes = []

    # Construct a graph for each DATA_DESC_ID
    for xds in xds_from_ms(args.ms,
                           columns=["UVW", "ANTENNA1", "ANTENNA2", "TIME"],
                           group_cols=["FIELD_ID", "DATA_DESC_ID"],
                           chunks={"row": args.row_chunks}):
        if xds.attrs['FIELD_ID'] != args.fieldid:
            continue
        
        # Extract frequencies from the spectral window associated
        # with this data descriptor id
        field = field_ds[xds.attrs['FIELD_ID']]
        ddid = ddid_ds[xds.attrs['DATA_DESC_ID']]
        spw = spw_ds[ddid.SPECTRAL_WINDOW_ID.values]
        pol = pol_ds[ddid.POLARIZATION_ID.values]
        frequency = spw.CHAN_FREQ.data

        corrs = pol.NUM_CORR.values

        lm = radec_to_lm(radec, field.PHASE_DIR.data)

        if args.exp_sign_convention == 'casa':
            uvw = -xds.UVW.data
        elif args.exp_sign_convention == 'thompson':
            uvw = xds.UVW.data
        else:
            raise ValueError("Invalid sign convention '%s'" % args.sign)

        if args.spectra:
            # flux density at reference frequency ...
            # ... for logarithmic polynomial functions
            if log_spec_ind: Is=da.log(stokes[:,0,None])*frequency[None,:]**0
            # ... or for ordinary polynomial functions
            else: Is=stokes[:,0,None]*frequency[None,:]**0
            # additional terms of SED ...
            for jj in range(spec_coeff.shape[1]):
                # ... for logarithmic polynomial functions
                if log_spec_ind: Is+=spec_coeff[:,jj,None]*da.log((frequency[None,:]/ref_freq[:,None])**(jj+1))
                # ... or for ordinary polynomial functions
                else: Is+=spec_coeff[:,jj,None]*(frequency[None,:]/ref_freq[:,None]-1)**(jj+1)
            if log_spec_ind: Is=da.exp(Is)
            Qs=da.zeros_like(Is)
            Us=da.zeros_like(Is)
            Vs=da.zeros_like(Is)
            spectrum=da.stack([Is,Qs,Us,Vs],axis=-1) # stack along new axis and make it the last axis of the new array
            spectrum=spectrum.rechunk(spectrum.chunks[:2] + (spectrum.shape[2],))

        log.info('-------------------------------------------')
        log.info('Nr sources        = {0:d}'.format(stokes.shape[0]))
        log.info('-------------------------------------------')
        log.info('stokes.shape      = {0:}'.format(stokes.shape))
        log.info('frequency.shape   = {0:}'.format(frequency.shape))
        if args.spectra: log.info('Is.shape          = {0:}'.format(Is.shape))
        if args.spectra: log.info('spectrum.shape    = {0:}'.format(spectrum.shape))

        # (source, row, frequency)
        phase = phase_delay(lm, uvw, frequency)
        # If at least one Gaussian component is present in the component list then all
        # sources are modelled as Gaussian components (Delta components have zero width)
        if gaussian_components: phase *= gaussian(uvw, frequency, gaussian_shape)
        # (source, frequency, corr_products)
        brightness = convert(spectrum if args.spectra else stokes, ["I", "Q", "U", "V"],
                             corr_schema(pol))

        log.info('brightness.shape  = {0:}'.format(brightness.shape))
        log.info('phase.shape       = {0:}'.format(phase.shape))
        log.info('-------------------------------------------')
        log.info('Attempting phase-brightness einsum with "{0:s}"'.format(einsum_schema(pol,args.spectra)))

        # (source, row, frequency, corr_products)
        jones = da.einsum(einsum_schema(pol,args.spectra), phase, brightness)
        log.info('jones.shape       = {0:}'.format(jones.shape))
        log.info('-------------------------------------------')
        if gaussian_components: log.info('Some Gaussian sources found')
        else: log.info('All sources are Delta functions')
        log.info('-------------------------------------------')

        # Identify time indices
        _, time_index = da.unique(xds.TIME.data, return_inverse=True)

        # Predict visibilities
        vis = predict_vis(time_index, xds.ANTENNA1.data, xds.ANTENNA2.data,
                          None, jones, None, None, None, None)

        # Reshape (2, 2) correlation to shape (4,)
        if corrs == 4:
            vis = vis.reshape(vis.shape[:2] + (4,))

        # Assign visibilities to MODEL_DATA array on the dataset
        model_data = xr.DataArray(vis, dims=["row", "chan", "corr"])
        xds = xds.assign(**{args.output_column: model_data})
        # Create a write to the table
        write = xds_to_table(xds, args.ms, [args.output_column])
        # Add to the list of writes
        writes.append(write)

    # Submit all graph computations in parallel
    if args.num_workers:
        with ProgressBar(), dask.config.set(num_workers=args.num_workers):
            dask.compute(writes)
    else:
        with ProgressBar():
            dask.compute(writes)
