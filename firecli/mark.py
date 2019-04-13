import json
import os
import sys

from math import sqrt

import click
from sqlalchemy.orm import aliased
from sqlalchemy.orm.exc import NoResultFound

import firecli
from firecli import firedb
from fireapi.model import Punkt, PunktInformation, PunktInformationType, Srid, Koordinat

from typing import Dict, List, Set, Tuple, IO


@click.group()
def mark():
    """Arbejd med markdatafiler"""
    pass


@mark.command()
@firecli.default_options()
@click.option(
    '-o', '--output', default='', type=click.Path(writable=True, readable=False, allow_dash=True),
    help='Sæt navn på outputfil'
)
@click.argument('filnavne', nargs=-1, type=click.File('rt'))
def gamaficer(filnavne: List[click.File('rt')], output: click.Path, **kwargs) -> None:
    """
    Omsæt inputfil(er) til GNU Gama-format

    FILNAVNE er navn(e) på inputfil(er), fx 'KDI2018vest.txt'

    Output skrives til en fil med samme fornavn, som første
    inputfil, men med '.xml' som efternavn.

    (Dette kan overstyres ved eksplicit at anføre et outputfilnavn
    med brug af option '-o NAVN')
    """

    # Generer et fornuftigt outputfilnavn
    if (output==''):
        fil = filnavne[0].name
        if (fil=='<stdin>'):
             output = '-'
        else:
            output = os.path.splitext(filnavne[0].name)[0] + '.xml'

    # Læs alle inputfiler og opbyg oversigter over hhv.
    # anvendte punkter og udførte observationer
    try:
        observationer = list()
        punkter = set()
        for fil in filnavne:
            for line in fil:
                if '#'!=line[0]:
                    continue
                line = line.lstrip('#').strip()
                tokens = line.split()
                assert len(tokens) == 9, "Malformed input line: "+line
                observationer.append(line)
                punkter.add(tokens[0])
                punkter.add(tokens[1])
    except AssertionError as e:
        firecli.print(str(e))
        click.Abort()
    except:
        firecli.print("Fejl ved læsning af fil")
        click.Abort()

    dvr90 = hent_sridid(firedb, "EPSG:5799")
    assert dvr90 != 0, "DVR90 (EPSG:5799) ikke fundet i srid-tabel"
    eksporter(output, observationer, punkter, dvr90)


def eksporter(output: str, observationer: List[str], punkter: Set[str], koteid: int) -> None:
    """Skriv geojson og Gama-XML outputfiler"""

    # Generer dict med (position, kote, kotevarians) og ident som nøgle
    punktinfo = dict()
    for ident in sorted(punkter):
        pinfo = punkt_information(ident)
        geo  = punkt_geometri(ident, pinfo)
        kote = punkt_kote(pinfo, koteid)
        (H, sH) = (0, 0) if kote is None else (kote.z, kote.sz)
        punktinfo[ident] = (geo[0], geo[1], H, sH)

    # Skriv punktfil i geojson-format
    with open("punkter.geojson", "wt") as punktfil:
        til_json = {
            'type': 'FeatureCollection',
            'Features': list(punkt_feature(punktinfo))
        }
        json.dump(til_json, punktfil, indent=4)

    # Skriv observationsfil i geojson-format
    with open("observationer.geojson", "wt") as obsfil:
        til_json = {
            'type': 'FeatureCollection',
            'Features': list(obs_feature(punktinfo, observationer))
        }
        json.dump(til_json, obsfil, indent=4)

    # Skriv Gama-inputfil i XML-format
    with open(output, "wt") as gamafil:
        xml_preamble(gamafil)
        xml_description(gamafil, "bla bla bla")
        xml_fixed_points(gamafil)
        for key, val in punktinfo.items():
            if key.startswith("G."):
                xml_point(gamafil, True, key, val)
        xml_adjusted_points(gamafil)
        for key, val in punktinfo.items():
            if key.startswith("G.")==False:
                xml_point(gamafil, False, key, val)
        xml_observations(gamafil)

        for obs in obs_feature(punktinfo, observationer):
            xml_obs(gamafil, obs)
        xml_postamble(gamafil)


def obs_feature(punkter: Dict, observationer: List[str]) -> Dict:
    """Omsæt observationsinformationer til JSON-egnet dict"""
    for obs in observationer:
        dele = obs.split()
        assert len(dele)==9, "Malformet observation: " + obs

        fra = punkter[dele[0]]
        til = punkter[dele[1]]
        # Endnu ikke registrerede punkter sendes ud i Kattegat
        if fra is None:
            fra = [11, 56, 0]
        if til is None:
            til = [11, 56, 0]

        # Reparer mistænkelig formateringsfejl
        if dele[5].endswith("-557"):
            dele[5] = dele[5][:-4]

        feature = {
            'type': 'Feature',
            'properties': {
               'fra': dele[0],
               'til': dele[1],
               'dist': float(dele[4]),
               'dH':  float(dele[5]),
               'setups': int(dele[8]),
               'journal': dele[6]
            },
            'geometry': {
                'type': 'LineString',
                'coordinates': [
                    [float(fra[0]), float(fra[1])],
                    [float(til[0]), float(til[1])]
                ]
            }
        }
        yield feature


def punkt_feature(punkter: Dict) -> Dict:
    """Omsæt punktinformationer til JSON-egnet dict"""
    for key, val in punkter.items():
        feature = {
            'type': 'Feature',
            'properties': {
               'id': key,
               'H':  val[2],
               'sH': val[3]
            },
            'geometry': {
                'type': 'Point',
                'coordinates': [val[0], val[1]]
            }
        }
        yield feature


def punkt_information(ident: str) -> PunktInformation:
    """Find alle informationer for et fikspunkt"""
    pi = aliased(PunktInformation)
    pit = aliased(PunktInformationType)
    try:
        punktinfo = (
            firedb.session.query(pi).filter(
                pit.name.startswith("IDENT:"),
                pi.tekst == ident
            ).first()
        )
    except NoResultFound:
        firecli.print(f"Error! {ident} not found!", fg="red", err=True)
        sys.exit(1)
    return punktinfo


def punkt_kote(punktinfo: PunktInformation, koteid: int) -> Koordinat:
    """Find aktuelle koordinatværdi for koordinattypen koteid"""
    for koord in punktinfo.punkt.koordinater:
        if (koord.sridid != koteid):
            continue
        if koord.registreringtil is None:
            return koord
    return None


def punkt_geometri(ident: str, punktinfo: PunktInformation) -> Tuple[float, float]:
    """Find placeringskoordinat for punkt"""
    try:
        geom = firedb.hent_geometri_objekt(punktinfo.punktid)
        # Turn the string "POINT (lon lat)" into the tuple "(lon, lat)"
        geo = eval(str(geom.geometri).lstrip("POINT ").replace(' ', ','))
        assert len(geo)==2, "Bad geometry format: " + str(geom.geometri)
    except NoResultFound:
        firecli.print(f"Error! Geometry for {ident} not found!", fg="red", err=True)
        sys.exit(1)
    return geo


# Bør nok være en del af API
def hent_sridid(db, srid: str) -> int:
    srider = db.hent_srider()
    for s in srider:
        if (s.name==srid):
            return s.sridid
    return 0


# XML-hjælpefunktioner herfra

def xml_preamble(fil: IO['wt']) -> None:
    fil.writelines(
        '<?xml version="1.0" ?><gama-local>\n'
        '<network angles="left-handed" axes-xy="en" epoch="0.0">\n'
        '<parameters\n'
        '    algorithm="gso" angles="400" conf-pr="0.95"\n'
        '    cov-band="0" ellipsoid="grs80" latitude="55.7" sigma-act="apriori"\n'
        '    sigma-apr="1.0" tol-abs="1000.0"\n'
        '    update-constrained-coordinates="no"\n'
        '/>\n\n'
    )

def xml_description(fil: IO['wt'], desc: str) -> None:
    fil.writelines(
        f"<description>\n{desc}\n</description>\n<points-observations>\n\n"
    )

def xml_fixed_points(fil: IO['wt']) -> None:
    fil.writelines("\n\n<!-- Fixed -->\n\n")

def xml_adjusted_points(fil: IO['wt']) -> None:
    fil.writelines("\n\n<!-- Adjusted -->\n\n")

def xml_observations(fil: IO['wt']) -> None:
    fil.writelines("\n\n<height-differences>\n\n")

def xml_postamble(fil: IO['wt']) -> None:
    fil.writelines(
        "\n</height-differences></points-observations></network></gama-local>"
    )


def xml_point(fil: IO['wt'], fix: bool, key: str, val: Dict) -> None:
    """skriv punkt i Gama XML-notation"""
    fixadj = 'fix="Z"' if fix==True else 'adj="z"'
    z = val[2]
    fil.write(f'<point {fixadj} id="{key}" z="{z}"/>\n')


def xml_obs(fil: IO['wt'], obs: Dict) -> None:
    """skriv observation i Gama XML-notation"""
    fra = obs['properties']['fra']
    til = obs['properties']['til']
    val = obs['properties']['dH']
    dist = obs['properties']['dist'] / 1000.0
    # TODO: Brug rette udtryk til opmålingstypen
    stdev = sqrt(dist)*0.6+0.01*obs['properties']['setups']
    fil.write(
        f'<dh from="{fra}" to="{til}" '
        f'val="{val:+.5f}" dist="{dist:.5f}" stdev="{stdev:.2f}"/>\n'
    )
