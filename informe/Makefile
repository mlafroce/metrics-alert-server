md_files ?= $(wildcard *.md)
tex_files ?= $(patsubst %.md,build/%.tex,$(md_files))
main_tex ?= "main.tex"
output_name ?= "informe"
output_file ?= $(output_name).pdf

.PHONY: pdf clean pre-build all

all: pre-build $(tex_files) $(output_file)

pre-build:
	mkdir -p build

build/%.tex: %.md
	pandoc $< -o $@
	sed -i s_,height=\\\\textheight__g $@

$(main_tex):

$(output_file): $(main_tex)
	pdflatex --jobname $(output_name) $<

clean:
	rm -rf build *.aux *.log *.out *.pdf *.toc $(tex_files)
