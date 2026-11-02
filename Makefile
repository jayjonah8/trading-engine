run:
	caffeinate -i cargo run

###################################################
###################################################

# Hyperliquid 

# Lists (not download) the first 20 objects in Hyperliquid's historical fills archive. (Cost almost nothing)
hyperliquid-list-history:
	aws s3api list-objects-v2 \
		--bucket hl-mainnet-node-data \
		--prefix node_fills_by_block \
		--max-keys 20 \
		--request-payer requester \
		--profile admin

# Downloads exactly one hour of Hyperliquid historical fills for testing. (Cost almost nothing)
#
# ONE S3 GetObject request
#         ↓
# ONE ~20.4 MiB compressed file
#         ↓
# ~/trading-data/hyperliquid/
hyperliquid-download-test-hour:
	mkdir -p ~/trading-data/hyperliquid/node_fills_by_block/hourly/20261015
	aws s3api get-object \
		--bucket hl-mainnet-node-data \
		--key node_fills_by_block/hourly/20261015/10.lz4 \
		--request-payer requester \
		--profile admin \
		~/trading-data/hyperliquid/node_fills_by_block/hourly/20261015/10.lz4

# Deletes all locally cached Hyperliquid historical archive data.
# This does not affect the remote Hyperliquid S3 archive.
hyperliquid-delete-history:
	rm -rf ~/trading-data/hyperliquid/node_fills_by_block

# Move data into backup folder (for small testing data / not deleted what I already have)
# mv ~/trading-data/hyperliquid/node_fills_by_block \
#   ~/trading-data/hyperliquid/test_data_backup


# ============================================================
# HYPERLIQUID HISTORICAL ARCHIVE SIZE
# ============================================================

# Estimates the compressed size and AWS download cost of one
# year of Hyperliquid historical fills without downloading data.
#
# Period: November 1, 2025 through October 31, 2026.
#
# Uses AWS S3 metadata requests with Requester Pays enabled.
# "caffinate" prevents macOS from sleeping while scanning historical S3 metadata.
hyperliquid-history-year-size:
	caffeinate -i python3 scripts/hyperliquid_history_size.py


# ============================================================
# HYPERLIQUID HISTORICAL DATE-RANGE DOWNLOAD
# ============================================================

# Downloads Hyperliquid historical fills for the UTC date range
# configured in scripts/hyperliquid_history_download.py.
#
# Existing archives are preserved and verified before skipping.
# Missing archives are downloaded automatically.
# Interrupted downloads can be restarted using the same command.
#
# Estimated AWS cost depends on the total compressed data size.
# "caffeinate" prevents macOS from sleeping while downloading historical data.
hyperliquid-download-range:
	caffeinate -i python3 scripts/hyperliquid_history_download.py



###################################################
###################################################

# Todo some values need to be updated

# Docker / AWS Commands:

AWS_URI=440744247268.dkr.ecr.us-west-1.amazonaws.com
REPO_NAME=hyper-ob-bot
PROFILE_NAME=admin
EC2_PUBLIC_IP=

## Docker Build Commands
dockerbuild :;docker build -t hyper-ob-bot .
## redo cache
dockerbuildcache :; docker build --no-cache -t hyper-ob-bot .
# The --platform linux/amd64 flag ensures compatibility with AWS’s Linux-based infrastructure. 
dockerbuildlinux :; docker buildx build --platform linux/amd64 --no-cache -t hyper-ob-bot .
dockerbuildlinuxfast :; docker buildx build --platform linux/amd64 -t hyper-ob-bot .

## Docker Run Commands
# dockerrun :; docker run -p 8080:8080 hyper-ob-bot
dockerrun :; docker run --env-file .env -p 8080:8080 ${REPO_NAME}
# run with the same localhost as my machine
dockerrunlocal :; docker run --env-file .env --network="host" hyper-ob-bot

# Start the container interactively: 
dockerinactive :; docker run -it --env-file .env hyper-ob-bot sh

## Docker Push to Registry Commands
dockertag :; docker tag ${REPO_NAME}:latest ${AWS_URI}/${REPO_NAME}:latest
dockerpush :; docker push ${AWS_URI}/${REPO_NAME}:latest


###################################################
###################################################


# AWS Authenticate
config :; aws configure sso
login :; aws ecr get-login-password --region us-west-1 --profile ${PROFILE_NAME} | docker login --username AWS --password-stdin ${AWS_URI} 

# SSH into EC2 instances: must locate the ssh key pair.pem path for this to work on run this command in its folder.
ssh :; ssh -i "ssh key pair.pem" ${EC2_PUBLIC_IP}

showprofile :; aws sts get-caller-identity --profile admin
# showprofile :; aws s3 ls --profile admin

identity :; aws sts get-caller-identity --profile admin


###################################################
###################################################


