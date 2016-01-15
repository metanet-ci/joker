PAPER = paper
VERBOSE = 0

# directories
FIGURE_DIR = figures
OUTPUT_DIR = output
STYLE_DIR = style
TEXT_DIR = text
BIB_OUT_FILE = $(OUTPUT_DIR)/bib-issues

# paths
BIB_PATH="$(TEXT_DIR)"
BST_PATH="$(STYLE_DIR)/sty:$(shell kpsewhich --progname=latex --show-path=.bst)"
TEX_PATH="$(TEXT_DIR):$(STYLE_DIR):$(STYLE_DIR)/sty:$(shell kpsewhich -progname=tex -show-path=tex)"

# options
BIBTEX_OPTS = -terse
DVIPS_OPTS = -q -G0 -tletter
LATEX_OPTS  = -output-directory=$(OUTPUT_DIR) -file-line-error -interaction=batchmode 
MAKEINDEX_OPTS = -q
PS2PDF_OPTS = -q -sPAPERSIZE=letter -dSAFER -dNOPAUSE -dBATCH -sDEVICE=pdfwrite -dPDFSETTINGS=/prepress \
              -dCompatibilityLevel=1.4 -dMaxSubsetPct=100 -dSubsetFonts=true 

#commands
LATEXC = max_print_line=1000 pdflatex
PRINT_LATEX_ERRORS = (cat $(OUTPUT_DIR)/$(PAPER).log | grep '^[^: ]*:[^: ]*:' || exit 0)
PRINT_LATEX_WARNINGS = (cat $(OUTPUT_DIR)/$(PAPER).log | grep -i 'latex warning' || exit 0)
PRINT_BIBTEX_WARNINGS = (cat $(OUTPUT_DIR)/*.blg | grep '^Warning' | sort | uniq > $(BIB_OUT_FILE) || exit 0)
PRINT_BIBTEX_NOTE = if [ -s $(BIB_OUT_FILE) ]; then \
                    echo "$(NOTE_LABEL)Bibliography warnings written to '$(BIB_OUT_FILE)'."; fi

#formats
NORM=
BOLD=
PS2PDF_LABEL   =[ps2pdf   ] 
DVIPS_LABEL    =[dvips    ] 
LATEX_LABEL    =[pdflatex ] 
BIBTEX_LABEL   =[bibtex   ] 
NOTE_LABEL     =[note     ] 
MAKEINDEX_LABEL=[makeindex] 
RM_LABEL       =[rm       ] 

SHELL:=/bin/bash

all: $(OUTPUT_DIR)/$(PAPER).pdf

show: $(OUTPUT_DIR)/$(PAPER).pdf
	@open $(OUTPUT_DIR)/$(PAPER).pdf &

$(OUTPUT_DIR)/$(PAPER).pdf: $(TEXT_DIR)/$(PAPER).tex                     \
                           $(shell find $(TEXT_DIR) -regex "[^.]*.tex")  \
                           $(shell find $(TEXT_DIR) -regex  "[^.]*.bib") \
                           $(shell find $(STYLE_DIR) -regex "[^.]*.tex") \
                           $(shell find $(FIGURE_DIR) -name "*.pdf") 
	@mkdir -p $(OUTPUT_DIR)

	@echo "$(LATEX_LABEL)Performing initial latex..."
	@TEXINPUTS=$(TEX_PATH) \
		$(LATEXC) $(LATEX_OPTS) $(PAPER).tex >/dev/null || \
			(rm -f $@ && $(PRINT_LATEX_ERRORS) && exit 1)

	@echo "$(BIBTEX_LABEL)Processing bibliography..."
	@BIBINPUTS=$(BIB_PATH) \
	 BSTINPUTS=$(BST_PATH) \
		bibtex $(BIBTEX_OPTS) output/$(PAPER) | grep -v "^Warning"; \
		if [ $${PIPESTATUS[0]} != 0 ]; then \
			rm -f $@ && exit 1; \
		fi 
	@-$(PRINT_BIBTEX_WARNINGS)
	@-$(PRINT_BIBTEX_NOTE)

#	@echo "$(MAKEINDEX_LABEL)Building index..."
#	@makeindex $(MAKEINDEX_OPTS) $(OUTPUT_DIR)/$(PAPER).idx || \
#		(rm -f $@ && exit 1)

	@echo "$(LATEX_LABEL)Performing secondary latex..."
	@TEXINPUTS=$(TEX_PATH) \
		$(LATEXC) $(LATEX_OPTS) $(PAPER).tex >/dev/null || \
			(rm -f $@ && $(PRINT_LATEX_ERRORS) && exit 1)

#	@echo "$(LATEX_LABEL)Performing ternary latex..."
#	@TEXINPUTS=$(TEX_PATH) \
#		latex $(LATEX_OPTS) $(PAPER).tex >/dev/null || \
#			(rm -f $@ && $(PRINT_LATEX_ERRORS) && exit 1)

	@echo "$(LATEX_LABEL)Performing final latex..."
	@TEXINPUTS=$(TEX_PATH) \
		$(LATEXC) $(LATEX_OPTS) $(PAPER).tex >/dev/null || \
			(rm -f $@ && $(PRINT_LATEX_ERRORS) && exit 1)
	@-$(PRINT_LATEX_WARNINGS)

clean:
	@echo '$(RM_LABEL)Cleaning project...'
	@rm -fr $(OUTPUT_DIR)
