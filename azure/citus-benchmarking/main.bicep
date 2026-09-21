// citus-benchmarking subscription-scope entry point.
//
// This template creates a dedicated resource group and deploys every Azure
// resource for one disposable benchmarking environment into that group.

targetScope = 'subscription'

@description('Name of the dedicated deployment resource group.')
param resourceGroupName string

@description('Canonical Azure region name, for example eastasia.')
param location string

@description('Linux virtual machine name.')
param vmName string

@description('Linux administrator username.')
param adminUsername string

@description('Azure virtual machine SKU.')
param vmSize string

@description('Existing OpenSSH public key used for VM login.')
@secure()
param sshPublicKey string

@description('IPv4 CIDR permitted to access SSH and PostgreSQL.')
param sourceAddressPrefix string

param virtualNetworkName string
param subnetName string
param networkSecurityGroupName string
param networkInterfaceName string
param publicIpName string
param osDiskName string

// One resource group owns the complete lifecycle of the environment.
resource deploymentResourceGroup 'Microsoft.Resources/resourceGroups@2024-03-01' = {
  name: resourceGroupName
  location: location
  tags: {
    project: 'citus-benchmarking'
    purpose: 'benchmarking'
    workload: 'citus'
    postgresVersion: '17'
    managedBy: 'create-citus-vm.sh'
  }
}

// Deploy VM and networking resources into the newly created resource group.
module infrastructure 'infrastructure.bicep' = {
  name: '${vmName}-infrastructure'
  scope: resourceGroup(deploymentResourceGroup.name)
  params: {
    location: location
    vmName: vmName
    adminUsername: adminUsername
    vmSize: vmSize
    sshPublicKey: sshPublicKey
    sourceAddressPrefix: sourceAddressPrefix
    virtualNetworkName: virtualNetworkName
    subnetName: subnetName
    networkSecurityGroupName: networkSecurityGroupName
    networkInterfaceName: networkInterfaceName
    publicIpName: publicIpName
    osDiskName: osDiskName
  }
}

output createdResourceGroupName string = deploymentResourceGroup.name
output deployedVmName string = vmName
