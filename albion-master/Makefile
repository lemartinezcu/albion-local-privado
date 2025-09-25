#Add iso code for any locales you want to support here (space separated)
# default is no locales
# LOCALES = af
LOCALES = "fr"
LANG := "en" ${LOCALES}

# If locales are enabled, set the name of the lrelease binary on your system. If
# you have trouble compiling the translations, you may have to specify the full path to
# lrelease
LRELEASE = lrelease
#LRELEASE = lrelease-qt4

PLUGINNAME = albion

QGISDIR=.local/share/QGIS/QGIS3/profiles/default
ROOTDIR=$(CURDIR)
FAMSADIR=$(HOME)/.local/share/FAMSA

default: zip

deploy: derase
	@echo
	@echo "------------------------------------------"
	@echo "Deploying plugin to your .qgis3 directory."
	@echo "------------------------------------------"
	# The deploy  target only works on unix like operating system where
	# the Python plugin directory is located at:
	# $HOME/$(QGISDIR)/python/plugins
	mkdir -p $(HOME)/$(QGISDIR)/python/plugins/$(PLUGINNAME)/help
	rsync -av --progress . $(HOME)/$(QGISDIR)/python/plugins/$(PLUGINNAME) \
	    --exclude ".*" \
	    --exclude doc_appstream \
	    --exclude media \
	    --exclude scripts \
	    --exclude tests \
	    --exclude Makefile \
	    --exclude *.zip \
	    --exclude quick_dev_install.md \
	    --exclude README.md \
	    --exclude "requirements*.txt" \
	    --filter="dir-merge,- .gitignore"

# The dclean target removes compiled python files from plugin directory
# also deletes any .git entry
dclean:
	@echo
	@echo "-----------------------------------"
	@echo "Removing any compiled python files."
	@echo "-----------------------------------"
	find $(HOME)/$(QGISDIR)/python/plugins/$(PLUGINNAME) -iname "*.pyc" -delete
	find $(HOME)/$(QGISDIR)/python/plugins/$(PLUGINNAME) -iname ".git" -prune -exec rm -Rf {} \;

derase:
	@echo
	@echo "-------------------------"
	@echo "Removing deployed plugin."
	@echo "-------------------------"
	rm -Rf $(HOME)/$(QGISDIR)/python/plugins/$(PLUGINNAME)

zip: deploy dclean
	@echo
	@echo "---------------------------"
	@echo "Creating plugin zip bundle."
	@echo "---------------------------"
	# The zip target deploys the plugin and creates a zip file with the deployed
	# content. You can then upload the zip file on http://plugins.qgis.org
	rm -f $(PLUGINNAME).zip
	cd $(HOME)/$(QGISDIR)/python/plugins; zip -9r $(CURDIR)/$(PLUGINNAME).zip $(PLUGINNAME)

package:
	# Create a zip package of the plugin named $(PLUGINNAME).zip.
	# This requires use of git (your plugin development directory must be a
	# git repository).
	# To use, pass a valid commit or tag as follows:
	#   make package VERSION=Version_0.3.2
	@echo
	@echo "------------------------------------"
	@echo "Exporting plugin to zip package.	"
	@echo "------------------------------------"
	rm -f $(PLUGINNAME).zip
	git archive --prefix=$(PLUGINNAME)/ -o $(PLUGINNAME).zip $(VERSION)
	echo "Created package: $(PLUGINNAME).zip"

$(FAMSADIR):
	# Recover FAMSA source code in Albion parent directory
	git clone -b 'v1.6.2' --depth 1 git@github.com:refresh-bio/FAMSA.git $(FAMSADIR)

famsa-patch: $(FAMSADIR)
	# Patch FAMSA with a hand-made matrix that fits the resistivity use case
	cd $(FAMSADIR) ; patch -p0 < $(ROOTDIR)/similog/FAMSA/patch_src-msa.cpp

$(FAMSADIR)/build: $(FAMSADIR)
	# Build FAMSA
	cd $(FAMSADIR); make
	rm -f $(HOME)/.local/bin/famsa
	ln -s $(FAMSADIR)/famsa $(HOME)/.local/bin/famsa

.PHONY: similog-install
similog-install: deploy $(FAMSADIR) famsa-patch $(FAMSADIR)/build
	# Install FAMSA and albion_similog
	@echo
	@echo "-----------------------------------"
	@echo "Install similog computing dependencies. "
	@echo "-----------------------------------"
	python3 -m pip install --upgrade albion_similog --user
