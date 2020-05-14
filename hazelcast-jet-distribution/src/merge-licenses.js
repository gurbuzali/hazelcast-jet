const fs = require('fs');
const path = require('path');

function groupBy(licensesFile, projects, licenses) {
    fs.readFileSync(licensesFile)
        .toString()
        .split('\n')
        .filter(s => s !== '')
        .map(s => s.split('|'))
        .forEach(([dep, licenseName, licenseText]) => {
            let fullLicense;
            if (licenseName === 'MIT') {
                fullLicense = 'MIT License'
            } else if (licenseName === 'Apache-2.0') {
                fullLicense = 'Apache License, Version 2.0'
            } else if (licenseName) {
                fullLicense = licenseName;
            }
            if (fullLicense) {
                if (!projects[fullLicense]) {
                    projects[fullLicense] = [];
                    licenses[fullLicense] = [];
                }
                const fullDep = dep.trim();
                if (fullDep) {
                    projects[fullLicense].push(fullDep);

                    if (licenseText) {
                        const fullLicenseText = licenseText.replace(/\[backslash\-n\]/g, '\n');
                        licenses[fullLicense].push(fullLicenseText);
                    } else {
                        licenses[fullLicense].push('N/A');
                    }
                }
            }
        })
}

function generateContent(projectsMap, licensesMap) {
    function tableSection(index, license, deps) {
        return `SECTION ${index + 1}: ${license}`
            + '\n'
            + `${deps.map(item => `   >>> ${item}\n`).join('')}`;
    }

    function noticeSection(index, license, texts) {
        return `--------------- SECTION ${index + 1}: ${license} ----------` +
            '\n\n' +
            `${license} is applicable to the following component(s).` +
            '\n\n' +
            `${texts.filter(text => text !== 'N/A')
                .map((text, index) => {
                    const projectName = projectsMap[license][index];
                    return `>>> ${projectName}`
                        + '\n\n'
                        + text + '\n'
                }).join('\n')}`;
    }

    const header = `=============== TABLE OF CONTENTS =============================
The following is a listing of the open source components detailed in
this document. This list is provided for your convenience; please read
further if you wish to review the copyright notice(s) and the full text
of the license associated with each component.


`;

    const tableOfContents = Object.entries(projectsMap)
        .map(([license, deps], index) => tableSection(index, license, deps))
        .join('\n\n');

    const notice = Object.entries(licensesMap)
        .map(([license, texts], index) => noticeSection(index, license, texts))
        .join('\n');
    return header + tableOfContents + '\n\n' + notice;
}

const projectsByLicense = {};
const licenseTextsByLicense = {};

let jsLicensesFile = path.resolve(process.argv[2]);
groupBy(jsLicensesFile, projectsByLicense, licenseTextsByLicense);

let javaLicensesFile = path.resolve(process.argv[3]);
groupBy(javaLicensesFile, projectsByLicense, licenseTextsByLicense);

const resultFilePath = path.resolve(process.argv[4]);

fs.writeFileSync(resultFilePath, generateContent(projectsByLicense, licenseTextsByLicense), 'utf8');

//fs.unlinkSync(jsLicensesFile);
//fs.unlinkSync(javaLicensesFile);