#!/bin/bash
set -e  # Exit immediately if a command exits with a non-zero status

echo "Starting the first batch of scripts..."

# First Batch of Scripts
node uscripts/installment-fip.js
node uscripts/installment-pif.js
node uscripts/overall-payment.js
node uscripts/vpenalty-details-pif.js
node uscripts/penalty-details.js
node uscripts/st-bill-details.js
node uscripts/st-loan-update.js

echo "First batch completed. Starting the second batch of scripts..."

# Second Batch of Scripts
# node uscripts/installment-payment-fip.js
# node uscripts/installment-payment-pif.js

echo "All scripts have been executed successfully."
