#!/bin/bash

bn='cdeveldoc'
makeindex $bn.idx
pdflatex $bn.tex
pdflatex $bn.tex
