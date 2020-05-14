<#-- To render the third-party file.
 Available context :
 - dependencyMap a collection of Map.Entry with
   key are dependencies (as a MavenProject) (from the maven project)
   values are licenses of each dependency (array of string)
 - licenseMap a collection of Map.Entry with
   key are licenses of each dependency (array of string)
   values are all dependencies using this license
-->

<#function artifact a>
    <#if a.name?index_of('Unnamed') &gt; -1>
        <#return (a.artifactId)?trim>
    <#else>
        <#return (a.name + "-" + a.version)?trim>
    </#if>
</#function>

<#function license l>
    <#assign printable = "">
    <#list l as line>
        <#assign printable = printable + "[backslash-n]" + line>
    </#list>
    <#return printable>
</#function>

<#if dependencyMap?size &gt; 0>
    <#list dependencyMap as e>
${artifact(e.getKey())}|${e.getValue()[0]?trim}|${license(e.getValue())}
    </#list>
</#if>